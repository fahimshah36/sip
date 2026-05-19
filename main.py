"""
Twilio Voice Backend — FastAPI
==============================

Architecture
------------
Browser (Twilio Voice JS SDK)
  │
  │  WebRTC (audio)
  ▼
Twilio Edge
  │
  ├─► /voice  (TwiML webhook)
  │     └─ <Start><Stream>  → WebSocket /media-stream/{parent_sid}  (audio monitoring)
  │     └─ <Dial>
  │           └─ <Number url="/child-twiml">  ← Gather wraps the child leg
  │
  ├─► /child-twiml  (runs on the PSTN side before bridging)
  │     └─ <Gather input="dtmf" num_digits=1 action="/dtmf-received">
  │
  ├─► /dtmf-received  (Twilio fires this when callee presses a digit)
  │     └─ pushes  "dtmf:5"  into the SSE queue for the browser
  │     └─ returns another <Gather> to keep listening
  │
  ├─► /child-status  (call progress: initiated / ringing / answered / completed)
  │
  └─► /call-events/{call_sid}  (SSE — browser subscribes for real-time events)

DTMF flow
---------
Callee presses 5
  → Twilio captures RFC-2833 event (reliable, fully out-of-band)
  → POST /dtmf-received  {"Digits": "5", "CallSid": "<child_sid>", ...}
  → Backend looks up parent_sid from child_to_parent map
  → Pushes  data: dtmf:5  into parent SSE queue
  → Browser EventSource fires and calls onDtmfDigit("5")

No Goertzel / no Web Audio frequency analysis needed on the frontend.
"""

import asyncio
import base64
import json
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, StreamingResponse
from twilio.jwt.access_token import AccessToken
from twilio.jwt.access_token.grants import VoiceGrant
from twilio.twiml.voice_response import Dial, Gather, Number, Start, VoiceResponse

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
)
logger = logging.getLogger("voice")

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------
ACCOUNT_SID   = os.environ["ACCOUNT_SID"]
AUTH_TOKEN    = os.environ["AUTH_TOKEN"]
API_KEY       = os.environ["API_KEY"]
API_SECRET    = os.environ["API_SECRET"]
TWIML_APP_SID = os.environ["TWIML_APP_SID"]
TWILIO_NUMBER = os.environ["TWILIO_NUMBER"]
BASE_URL      = os.environ["BASE_URL"].rstrip("/")

# ---------------------------------------------------------------------------
# In-memory call state
# Replace with Redis if running more than one worker process.
#
# call_sid (parent leg) → asyncio.Queue[str]
#   Events pushed:
#     "initiated"      child leg created
#     "ringing"        callee phone is ringing
#     "answered"       callee picked up
#     "completed"      call ended normally
#     "failed" etc.    other terminal states
#     "dtmf:<digit>"   callee pressed a key  e.g. "dtmf:5"
#
# child_to_parent: child call SID → parent call SID
# ---------------------------------------------------------------------------
call_status_queues: dict[str, asyncio.Queue] = {}
child_to_parent:   dict[str, str]            = {}
_cleanup_tasks:    dict[str, asyncio.Task]   = {}

QUEUE_TTL       = 300   # seconds to keep state after a call ends
TERMINAL_STATES = {"completed", "failed", "busy", "no-answer", "canceled"}


def _get_or_create_queue(call_sid: str) -> asyncio.Queue:
    if call_sid not in call_status_queues:
        call_status_queues[call_sid] = asyncio.Queue(maxsize=100)
    return call_status_queues[call_sid]


async def _do_cleanup(call_sid: str) -> None:
    await asyncio.sleep(QUEUE_TTL)
    call_status_queues.pop(call_sid, None)
    _cleanup_tasks.pop(call_sid, None)
    stale = [c for c, p in child_to_parent.items() if p == call_sid]
    for c in stale:
        child_to_parent.pop(c, None)
    logger.info(f"[cleanup] removed state for parent={call_sid}")


def _schedule_cleanup(call_sid: str) -> None:
    if call_sid not in _cleanup_tasks:
        _cleanup_tasks[call_sid] = asyncio.create_task(_do_cleanup(call_sid))


def _push_event(parent_sid: str, event: str) -> None:
    """Non-blocking push into the parent SSE queue. Silently drops if full."""
    if not parent_sid:
        return
    q = _get_or_create_queue(parent_sid)
    try:
        q.put_nowait(event)
    except asyncio.QueueFull:
        logger.warning(
            f"[queue] full — dropped event={event!r} parent={parent_sid}"
        )


# ---------------------------------------------------------------------------
# App lifecycle
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(_app: FastAPI):
    yield
    for task in list(_cleanup_tasks.values()):
        task.cancel()


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# GET /token
# Returns a short-lived Twilio Access Token for the browser JS SDK.
# ---------------------------------------------------------------------------
@app.get("/token")
def get_token():
    at = AccessToken(
        ACCOUNT_SID, API_KEY, API_SECRET,
        identity="visitor",
        ttl=3600,
    )
    at.add_grant(VoiceGrant(
        outgoing_application_sid=TWIML_APP_SID,
        incoming_allow=True,
    ))
    return {"token": at.to_jwt()}


# ---------------------------------------------------------------------------
# POST /voice
# Twilio calls this when the browser initiates an outbound call via the SDK.
#
# TwiML produced:
#
#   <Response>
#     <Start>
#       <Stream name="monitor" url="wss://…/media-stream/{parent_sid}"
#               track="both_tracks"/>
#     </Start>
#     <Dial callerId="…" answerOnBridge="false" action="/dial-complete"
#           timeout="30" timeLimit="14400">
#       <Number url="/child-twiml" statusCallback="/child-status"
#               statusCallbackEvent="initiated ringing answered completed">
#         +15551234567
#       </Number>
#     </Dial>
#   </Response>
#
# Key decisions
# -------------
# • answer_on_bridge=False  avoids early-media race with <Start><Stream>.
# • <Number url="/child-twiml">  tells Twilio to run /child-twiml on the
#   PSTN leg after the callee answers but before bridging.  That is where
#   <Gather> lives so DTMF comes in via webhook, not in-band audio.
# ---------------------------------------------------------------------------
@app.post("/voice")
async def voice(req: Request):
    form       = await req.form()
    to         = form.get("To", "")
    parent_sid = form.get("CallSid", "")

    _get_or_create_queue(parent_sid)

    ws_base = (
        BASE_URL
        .replace("https://", "wss://")
        .replace("http://",  "ws://")
    )

    response = VoiceResponse()

    # ── Audio monitoring (unidirectional, both sides) ────────────────────────
    start = Start()
    start.stream(
        name="monitor",
        url=f"{ws_base}/media-stream/{parent_sid}",
        track="both_tracks",
    )
    response.append(start)

    # ── Outbound PSTN dial ───────────────────────────────────────────────────
    dial = Dial(
        caller_id=TWILIO_NUMBER,
        answer_on_bridge=False,
        action=f"{BASE_URL}/dial-complete",
        method="POST",
        timeout=30,
        time_limit=14400,
    )
    dial.append(Number(
        to,
        url=f"{BASE_URL}/child-twiml",          # DTMF <Gather> runs here
        method="POST",
        status_callback=f"{BASE_URL}/child-status",
        status_callback_method="POST",
        status_callback_event="initiated ringing answered completed",
    ))
    response.append(dial)

    logger.info(f"[voice] new call  parent={parent_sid}  to={to}")
    return Response(content=str(response), media_type="text/xml")


# ---------------------------------------------------------------------------
# POST /child-twiml
# Runs on the PSTN (callee) leg immediately after they answer,
# before Twilio bridges audio to the browser.
#
# We place a <Gather> here with num_digits=1 and action=/dtmf-received.
# Every single keypress fires a webhook — no in-band frequency analysis.
# Twilio passes ParentCallSid so we know which browser to notify.
# ---------------------------------------------------------------------------
@app.post("/child-twiml")
async def child_twiml(req: Request):
    form       = await req.form()
    child_sid  = form.get("CallSid", "")
    parent_sid = form.get("ParentCallSid", "")

    if child_sid and parent_sid:
        child_to_parent[child_sid] = parent_sid
        logger.info(f"[child-twiml] child={child_sid}  parent={parent_sid}")

    response = VoiceResponse()

    # num_digits=1  → fires the action URL after every single keypress
    # finish_on_key=""  → no key terminates the gather early
    # timeout=3600   → keep listening for the whole call duration
    gather = Gather(
        input="dtmf",
        action=f"{BASE_URL}/dtmf-received",
        method="POST",
        num_digits=1,
        finish_on_key="",
        timeout=3600,
    )
    response.append(gather)

    # Safety fallback — should never reach here with num_digits=1
    response.pause(length=1)

    return Response(content=str(response), media_type="text/xml")


# ---------------------------------------------------------------------------
# POST /dtmf-received
# Twilio fires this every time the callee presses a digit.
#
# We push "dtmf:<digit>" into the parent SSE queue, then return another
# <Gather> so Twilio keeps listening for further keypresses.
# ---------------------------------------------------------------------------
@app.post("/dtmf-received")
async def dtmf_received(req: Request):
    form      = await req.form()
    digits    = form.get("Digits", "")
    child_sid = form.get("CallSid", "")

    parent_sid = child_to_parent.get(child_sid)

    logger.info(
        f"[dtmf] digit={digits!r}  child={child_sid}  parent={parent_sid}"
    )

    if digits and parent_sid:
        for digit in digits:        # num_digits=1 so usually one char, but be safe
            _push_event(parent_sid, f"dtmf:{digit}")

    # Return another <Gather> so the next keypress is also captured
    response = VoiceResponse()
    gather = Gather(
        input="dtmf",
        action=f"{BASE_URL}/dtmf-received",
        method="POST",
        num_digits=1,
        finish_on_key="",
        timeout=3600,
    )
    response.append(gather)
    response.pause(length=1)

    return Response(content=str(response), media_type="text/xml")


# ---------------------------------------------------------------------------
# POST /child-status
# Call-progress webhooks for the PSTN leg.
# Maps Twilio status names → frontend event names and pushes to SSE queue.
# ---------------------------------------------------------------------------
@app.post("/child-status")
async def child_status(req: Request):
    form        = await req.form()
    call_status = form.get("CallStatus", "")
    parent_sid  = form.get("ParentCallSid", "")
    child_sid   = form.get("CallSid", "")

    logger.info(
        f"[child-status] status={call_status}"
        f"  child={child_sid}  parent={parent_sid}"
    )

    # Normalise Twilio status → frontend event name
    status_map: dict[str, str] = {
        "initiated":   "initiated",
        "ringing":     "ringing",
        "in-progress": "answered",
        "completed":   "completed",
        "failed":      "failed",
        "busy":        "busy",
        "no-answer":   "no-answer",
        "canceled":    "canceled",
    }
    event = status_map.get(call_status, call_status)

    if parent_sid:
        # Backup child→parent registration in case /child-twiml was missed
        if child_sid and child_sid not in child_to_parent:
            child_to_parent[child_sid] = parent_sid

        _push_event(parent_sid, event)

        if call_status in TERMINAL_STATES:
            _schedule_cleanup(parent_sid)
            child_to_parent.pop(child_sid, None)

    return Response(content="", status_code=200)


# ---------------------------------------------------------------------------
# WebSocket /media-stream/{call_sid}
# Receives the forked audio stream from <Start><Stream>.
# DTMF is NOT detected here — it comes via /dtmf-received above.
# This handler is for audio monitoring / STT / recording pipelines.
# ---------------------------------------------------------------------------
@app.websocket("/media-stream/{call_sid}")
async def media_stream(websocket: WebSocket, call_sid: str):
    await websocket.accept()
    logger.info(f"[ws] connected  call_sid={call_sid}")

    # Internal queue decouples the fast receiver from the slower processor.
    # If the processor falls behind, frames are dropped (not blocked).
    audio_queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=500)
    stream_sid:  str | None           = None

    async def receiver() -> None:
        nonlocal stream_sid
        try:
            while True:
                raw   = await websocket.receive_text()
                msg   = json.loads(raw)
                event = msg.get("event", "")

                if event == "connected":
                    logger.info(f"[ws] handshake OK  call_sid={call_sid}")

                elif event == "start":
                    meta       = msg.get("start", {})
                    stream_sid = meta.get("streamSid")
                    logger.info(
                        f"[ws] stream started  sid={stream_sid}"
                        f"  tracks={meta.get('tracks')}  call={call_sid}"
                    )

                elif event == "media":
                    payload = msg.get("media", {}).get("payload", "")
                    if payload and not audio_queue.full():
                        # 8 kHz µ-law PCM, base64-encoded
                        audio_queue.put_nowait(base64.b64decode(payload))

                elif event == "stop":
                    logger.info(f"[ws] stream stopped  call_sid={call_sid}")
                    break

                # <Start><Stream> never sends dtmf events — those come via
                # the /dtmf-received webhook.

        except WebSocketDisconnect:
            logger.info(f"[ws] client disconnected  call_sid={call_sid}")
        except Exception as exc:
            logger.error(f"[ws] receiver error: {exc}")

    async def processor() -> None:
        """
        Consume µ-law audio frames without blocking the receiver loop.
        Plug your STT / recording / analysis logic in here.
        """
        while True:
            try:
                _chunk = await asyncio.wait_for(audio_queue.get(), timeout=10.0)
                # TODO: forward _chunk to Deepgram / Whisper / S3 / etc.
                audio_queue.task_done()
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

    async def keepalive() -> None:
        """Ping Twilio every 20 s to prevent load-balancer idle timeout."""
        try:
            while True:
                await asyncio.sleep(20)
                await websocket.send_text(json.dumps({"event": "ping"}))
        except Exception:
            pass  # socket already closed

    recv_task      = asyncio.create_task(receiver())
    process_task   = asyncio.create_task(processor())
    keepalive_task = asyncio.create_task(keepalive())

    try:
        await recv_task
    finally:
        process_task.cancel()
        keepalive_task.cancel()
        logger.info(f"[ws] all tasks done  call_sid={call_sid}")


# ---------------------------------------------------------------------------
# GET /call-events/{call_sid}
# Server-Sent Events — the browser subscribes here to get real-time updates.
#
# Event payload (text after "data: "):
#   "initiated"    child leg created
#   "ringing"      callee phone ringing
#   "answered"     callee picked up
#   "completed"    call ended
#   "busy"         line busy
#   "no-answer"    callee didn't pick up
#   "failed"       call failed
#   "canceled"     caller hung up before answer
#   "dtmf:5"       callee pressed digit 5  ← this is what your hook reads
# ---------------------------------------------------------------------------
@app.get("/call-events/{call_sid}")
async def call_events(call_sid: str):
    queue = _get_or_create_queue(call_sid)

    async def stream():
        try:
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=25)
                    yield f"data: {event}\n\n"
                    if event in TERMINAL_STATES:
                        break
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"   # SSE comment line — browser ignores
        except asyncio.CancelledError:
            pass

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control":    "no-cache",
            "X-Accel-Buffering": "no",      # disable nginx response buffering
            "Connection":       "keep-alive",
        },
    )


# ---------------------------------------------------------------------------
# POST /dial-complete
# Twilio calls this when the <Dial> verb finishes (either side hung up).
# ---------------------------------------------------------------------------
@app.post("/dial-complete")
async def dial_complete(req: Request):
    form       = await req.form()
    parent_sid = form.get("CallSid", "")
    status     = form.get("DialCallStatus", "")

    logger.info(f"[dial-complete] status={status}  parent={parent_sid}")

    if parent_sid and status in TERMINAL_STATES:
        _push_event(parent_sid, status)
        _schedule_cleanup(parent_sid)

    return Response(content=str(VoiceResponse()), media_type="text/xml")


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------
@app.get("/health")
def health():
    return {
        "status":           "ok",
        "active_calls":     len(call_status_queues),
        "tracked_children": len(child_to_parent),
    }