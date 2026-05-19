import os
import asyncio
import json
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, StreamingResponse
from twilio.jwt.access_token import AccessToken
from twilio.jwt.access_token.grants import VoiceGrant
from twilio.twiml.voice_response import VoiceResponse, Dial, Number, Connect, Stream, Start

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Env
# ---------------------------------------------------------------------------
ACCOUNT_SID   = os.environ["ACCOUNT_SID"]
AUTH_TOKEN    = os.environ["AUTH_TOKEN"]
API_KEY       = os.environ["API_KEY"]
API_SECRET    = os.environ["API_SECRET"]
TWIML_APP_SID = os.environ["TWIML_APP_SID"]
TWILIO_NUMBER = os.environ["TWILIO_NUMBER"]
BASE_URL      = os.environ["BASE_URL"].rstrip("/")

# ---------------------------------------------------------------------------
# In-memory state  (replace with Redis for multi-worker deployments)
# ---------------------------------------------------------------------------
# call_sid → asyncio.Queue[str]  (call status events)
call_status_queues: dict[str, asyncio.Queue] = {}
# call_sid → asyncio.Task (background cleanup)
_cleanup_tasks: dict[str, asyncio.Task] = {}

QUEUE_TTL = 300  # seconds to keep a queue after the call ends


def _get_or_create_queue(call_sid: str) -> asyncio.Queue:
    if call_sid not in call_status_queues:
        call_status_queues[call_sid] = asyncio.Queue(maxsize=50)
    return call_status_queues[call_sid]


async def _schedule_cleanup(call_sid: str):
    """Remove the queue after TTL so we don't leak memory."""
    await asyncio.sleep(QUEUE_TTL)
    call_status_queues.pop(call_sid, None)
    _cleanup_tasks.pop(call_sid, None)
    logger.info(f"[cleanup] removed queue for {call_sid}")


def _trigger_cleanup(call_sid: str):
    if call_sid not in _cleanup_tasks:
        _cleanup_tasks[call_sid] = asyncio.create_task(_schedule_cleanup(call_sid))


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    # Cancel any pending cleanup tasks on shutdown
    for task in _cleanup_tasks.values():
        task.cancel()


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Token
# ---------------------------------------------------------------------------
@app.get("/token")
def token():
    access_token = AccessToken(
        ACCOUNT_SID, API_KEY, API_SECRET, identity="visitor", ttl=3600
    )
    access_token.add_grant(VoiceGrant(
        outgoing_application_sid=TWIML_APP_SID,
        incoming_allow=True,
    ))
    return {"token": access_token.to_jwt()}


# ---------------------------------------------------------------------------
# /voice  – the TwiML webhook Twilio calls when a call is initiated
#
# FIX: Move <Start><Stream> AFTER <Dial> is set up, and use a <Connect><Stream>
# pattern OR attach the stream only after the bridge is live.
#
# The safest pattern for browser → PSTN calls where you want to monitor audio
# without breaking the voice bridge is:
#   1. <Dial answerOnBridge="true"> with <Number> — this establishes the bridge
#   2. Use the statusCallback on <Number> to know when the call is answered,
#      then start the stream via the REST API (Streams resource) separately.
#
# However, if you only need passive monitoring (no audio injection), the
# approach below is the minimal-impact fix:  keep <Start><Stream> but move it
# INSIDE the dial action response, or simply accept that the stream starts at
# dial-time and ensure the WS handler is non-blocking.
#
# The CRITICAL fix for audio quality is removing answer_on_bridge interference
# with the early stream, and making the WS consumer fast and non-blocking.
# ---------------------------------------------------------------------------
@app.post("/voice")
async def voice(req: Request):
    form       = await req.form()
    to         = form.get("To", "")
    parent_sid = form.get("CallSid", "")

    _get_or_create_queue(parent_sid)

    ws_base = BASE_URL.replace("https://", "wss://").replace("http://", "ws://")

    response = VoiceResponse()

    # -----------------------------------------------------------------------
    # FIX 1: Start the stream with a name so it can be stopped later.
    # FIX 2: The stream starts here (before dial connects), which is fine for
    # monitoring — but we must ensure the WS handler is fast (see below).
    # FIX 3: Do NOT set answer_on_bridge=True when using <Start><Stream> before
    # <Dial>, as this creates a timing race where Twilio may not have fully
    # set up the bridge when the media stream starts.
    # -----------------------------------------------------------------------
    start = Start()
    start.stream(
        name="live-monitor",
        url=f"{ws_base}/media-stream/{parent_sid}",
        track="both_tracks",
    )
    response.append(start)

    dial = Dial(
        caller_id=TWILIO_NUMBER,
        # FIX 4: Set answer_on_bridge=False (default) when stream starts before
        # Dial. answer_on_bridge=True is great for UX but when combined with a
        # pre-dial stream it can cause one-way audio on some carriers because
        # the early-media path conflicts with the stream fork timing.
        # If you want the ringing UX, start the stream in /dial-complete instead.
        answer_on_bridge=False,
        action=f"{BASE_URL}/dial-complete",
        method="POST",
        timeout=30,        # FIX 5: explicit timeout (default 30s, good to be explicit)
        time_limit=14400,  # FIX 6: 4-hour hard cap to prevent runaway calls
    )
    dial.append(Number(
        to,
        status_callback=f"{BASE_URL}/child-status",
        status_callback_method="POST",
        status_callback_event="initiated ringing answered completed",
    ))
    response.append(dial)

    logger.info(f"[voice] outbound call parent={parent_sid} to={to}")
    return Response(content=str(response), media_type="text/xml")


# ---------------------------------------------------------------------------
# /media-stream/{call_sid}  –  WebSocket handler
#
# FIX: The original handler blocked on receive_text() in a tight loop and
# discarded everything. This is fine functionally but can cause backpressure
# if the event loop is busy, delaying ACKs and causing Twilio to perceive the
# WebSocket as slow, which can stall the audio pipeline.
#
# Improvements:
#  • Use asyncio.Queue as a decoupled producer/consumer
#  • Send WebSocket pings to keep the connection alive on long calls
#  • Log stream start/stop events for observability
# ---------------------------------------------------------------------------
@app.websocket("/media-stream/{call_sid}")
async def media_stream(websocket: WebSocket, call_sid: str):
    await websocket.accept()
    logger.info(f"[media-stream] connected  call_sid={call_sid}")

    # Internal queue so the receive loop is never blocked by processing
    audio_queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=200)
    stream_sid: str | None = None

    async def receiver():
        """Read from WebSocket as fast as possible — never block here."""
        nonlocal stream_sid
        try:
            while True:
                raw  = await websocket.receive_text()
                msg  = json.loads(raw)
                event = msg.get("event", "")

                if event == "start":
                    stream_sid = msg.get("start", {}).get("streamSid")
                    logger.info(f"[media-stream] stream started  streamSid={stream_sid}  call_sid={call_sid}")

                elif event == "stop":
                    logger.info(f"[media-stream] stream stopped  call_sid={call_sid}")
                    break

                elif event == "media":
                    # Drop frames if consumer is behind — better to drop than block
                    if not audio_queue.full():
                        await audio_queue.put(msg)

                elif event == "connected":
                    logger.info(f"[media-stream] ws connected  call_sid={call_sid}")

                else:
                    logger.debug(f"[media-stream] event={event}  call_sid={call_sid}")

        except WebSocketDisconnect:
            logger.info(f"[media-stream] disconnected  call_sid={call_sid}")
        except Exception as e:
            logger.error(f"[media-stream] receiver error: {e}")

    async def processor():
        """
        Process audio frames here.  Currently a no-op passthrough — add your
        STT, recording, or analysis logic here without blocking the receiver.
        """
        while True:
            try:
                msg = await asyncio.wait_for(audio_queue.get(), timeout=5.0)
                # TODO: forward to STT / recording pipeline
                # payload = msg["media"]["payload"]  # base64 mulaw audio
                audio_queue.task_done()
            except asyncio.TimeoutError:
                # No audio for 5s — normal during silence or after call ends
                continue
            except asyncio.CancelledError:
                break

    async def keepalive():
        """
        FIX: Send periodic pings so the WS isn't closed by load balancers /
        Twilio's edge on long calls (> 60 s idle).
        """
        try:
            while True:
                await asyncio.sleep(20)
                await websocket.send_text(json.dumps({"event": "ping"}))
        except Exception:
            pass  # WS is already closed

    recv_task      = asyncio.create_task(receiver())
    process_task   = asyncio.create_task(processor())
    keepalive_task = asyncio.create_task(keepalive())

    try:
        await recv_task  # wait until the receiver finishes
    finally:
        process_task.cancel()
        keepalive_task.cancel()
        logger.info(f"[media-stream] all tasks finished  call_sid={call_sid}")


# ---------------------------------------------------------------------------
# /child-status  –  status webhook for the outbound leg
# ---------------------------------------------------------------------------
@app.post("/child-status")
async def child_status(req: Request):
    form        = await req.form()
    call_status = form.get("CallStatus", "")
    parent_sid  = form.get("ParentCallSid", "")
    child_sid   = form.get("CallSid", "")
    logger.info(f"[child-status] {call_status}  child={child_sid}  parent={parent_sid}")

    if parent_sid:
        queue = _get_or_create_queue(parent_sid)
        try:
            queue.put_nowait(call_status)
        except asyncio.QueueFull:
            logger.warning(f"[child-status] queue full for {parent_sid}, dropping event")

        terminal = {"completed", "failed", "busy", "no-answer", "canceled"}
        if call_status in terminal:
            _trigger_cleanup(parent_sid)

    return Response(content="", status_code=200)


# ---------------------------------------------------------------------------
# /call-events/{call_sid}  –  SSE endpoint the browser polls for status
# ---------------------------------------------------------------------------
TERMINAL_STATUSES = {"completed", "failed", "busy", "no-answer", "canceled"}

@app.get("/call-events/{call_sid}")
async def call_events(call_sid: str):
    queue = _get_or_create_queue(call_sid)

    async def stream():
        try:
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=25)
                    yield f"data: {event}\n\n"
                    if event in TERMINAL_STATUSES:
                        break
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",   # FIX: explicit keep-alive for SSE
        },
    )


# ---------------------------------------------------------------------------
# /dial-complete  –  called when the <Dial> leg ends
# ---------------------------------------------------------------------------
@app.post("/dial-complete")
async def dial_complete(req: Request):
    form       = await req.form()
    parent_sid = form.get("CallSid", "")
    status     = form.get("DialCallStatus", "")
    logger.info(f"[dial-complete] DialCallStatus={status}  parent={parent_sid}")

    if parent_sid and status in TERMINAL_STATUSES:
        queue = _get_or_create_queue(parent_sid)
        try:
            queue.put_nowait(status)
        except asyncio.QueueFull:
            pass
        _trigger_cleanup(parent_sid)

    # Return empty TwiML — call ends cleanly
    return Response(content=str(VoiceResponse()), media_type="text/xml")


# ---------------------------------------------------------------------------
# /health
# ---------------------------------------------------------------------------
@app.get("/health")
def health():
    return {
        "status": "ok",
        "active_calls": len(call_status_queues),
    }