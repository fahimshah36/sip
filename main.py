import os
import asyncio
import json
from collections import defaultdict

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, StreamingResponse
from twilio.jwt.access_token import AccessToken
from twilio.jwt.access_token.grants import VoiceGrant
from twilio.twiml.voice_response import VoiceResponse, Dial, Number, Start, Gather
from twilio.rest import Client

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

ACCOUNT_SID   = os.environ["ACCOUNT_SID"]
AUTH_TOKEN    = os.environ["AUTH_TOKEN"]
API_KEY       = os.environ["API_KEY"]
API_SECRET    = os.environ["API_SECRET"]
TWIML_APP_SID = os.environ["TWIML_APP_SID"]
TWILIO_NUMBER = os.environ["TWILIO_NUMBER"]
BASE_URL      = os.environ["BASE_URL"]

twilio_client = Client(ACCOUNT_SID, AUTH_TOKEN)

call_status_queues: dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)
# Map child SID -> parent SID so DTMF callback can find the right queue
child_to_parent: dict[str, str] = {}


# ── Token ──────────────────────────────────────────────────────────────────────

@app.get("/token")
def token():
    access_token = AccessToken(
        ACCOUNT_SID, API_KEY, API_SECRET, identity="visitor"
    )
    access_token.add_grant(VoiceGrant(
        outgoing_application_sid=TWIML_APP_SID,
        incoming_allow=True,
    ))
    return {"token": access_token.to_jwt()}


# ── Voice webhook ──────────────────────────────────────────────────────────────

@app.post("/voice")
async def voice(req: Request):
    form       = await req.form()
    to         = form.get("To", "")
    parent_sid = form.get("CallSid", "")

    _ = call_status_queues[parent_sid]

    ws_base = BASE_URL.replace("https://", "wss://").replace("http://", "ws://")

    response = VoiceResponse()

    start = Start()
    start.stream(
        url=f"{ws_base}/media-stream/{parent_sid}",
        track="both_tracks",
    )
    response.append(start)

    dial = Dial(
        caller_id=TWILIO_NUMBER,
        answer_on_bridge=True,
        action=f"{BASE_URL}/dial-complete",
        method="POST",
    )
    dial.append(Number(
        to,
        status_callback=f"{BASE_URL}/child-status",
        status_callback_method="POST",
        # Remove dtmf from here — it doesn't work on Number
        status_callback_event="initiated ringing answered completed",
    ))
    response.append(dial)

    return Response(content=str(response), media_type="text/xml")


# ── Child status: detect "answered" then redirect child leg to gather TwiML ────

@app.post("/child-status")
async def child_status(req: Request):
    form        = await req.form()
    call_status = form.get("CallStatus", "")
    parent_sid  = form.get("ParentCallSid", "")
    child_sid   = form.get("CallSid", "")

    print(f"[child-status] {call_status}  child={child_sid}  parent={parent_sid}")

    if parent_sid and parent_sid in call_status_queues:
        await call_status_queues[parent_sid].put(call_status)

    # When child answers, redirect it to a looping Gather TwiML
    # This runs in the background so we don't block the status callback response
    if call_status == "in-progress" and child_sid and parent_sid:
        child_to_parent[child_sid] = parent_sid
        asyncio.create_task(redirect_child_to_gather(child_sid))

    return Response(content="", status_code=200)


async def redirect_child_to_gather(child_sid: str):
    # Small delay to let the bridge fully establish first
    await asyncio.sleep(1)
    try:
        twilio_client.calls(child_sid).update(
            url=f"{BASE_URL}/child-gather",
            method="POST",
        )
        print(f"[gather] redirected child {child_sid} to gather TwiML")
    except Exception as e:
        print(f"[gather] redirect failed: {e}")


# ── Gather TwiML for child leg ─────────────────────────────────────────────────

@app.post("/child-gather")
async def child_gather(req: Request):
    """
    Looping Gather on the child leg.
    numDigits=1 catches each keypress immediately.
    action posts back here so we loop forever.
    timeout=600 = 10 minutes, effectively keeps gathering for call duration.
    """
    form      = await req.form()
    digit     = form.get("Digits", "")
    child_sid = form.get("CallSid", "")

    print(f"[child-gather] digit={digit!r}  child={child_sid}")

    if digit and child_sid in child_to_parent:
        parent_sid = child_to_parent[child_sid]
        await call_status_queues[parent_sid].put(f"dtmf:{digit}")
        print(f"[child-gather] forwarded dtmf:{digit} to parent {parent_sid}")

    # Loop: keep gathering
    response = VoiceResponse()
    gather = Gather(
        num_digits=1,
        action=f"{BASE_URL}/child-gather",
        method="POST",
        timeout=600,
        action_on_empty_result=True,
    )
    response.append(gather)

    return Response(content=str(response), media_type="text/xml")


# ── Media Streams WebSocket ────────────────────────────────────────────────────

@app.websocket("/media-stream/{call_sid}")
async def media_stream(websocket: WebSocket, call_sid: str):
    await websocket.accept()
    try:
        while True:
            data = await websocket.receive_text()
            msg  = json.loads(data)
            if msg.get("event") != "media":
                print(f"[media-stream] event={msg.get('event')}  call_sid={call_sid}")
    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"[media-stream] error: {e}")


# ── SSE stream ─────────────────────────────────────────────────────────────────

@app.get("/call-events/{call_sid}")
async def call_events(call_sid: str):
    queue = call_status_queues[call_sid]

    async def stream():
        try:
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=25)
                    yield f"data: {event}\n\n"
                    if event in ("completed", "failed", "busy", "no-answer", "canceled"):
                        break
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ── Dial complete ──────────────────────────────────────────────────────────────

@app.post("/dial-complete")
async def dial_complete(req: Request):
    form       = await req.form()
    parent_sid = form.get("CallSid", "")
    status     = form.get("DialCallStatus", "")
    print(f"[dial-complete] DialCallStatus={status}  parent={parent_sid}")

    if parent_sid and parent_sid in call_status_queues:
        if status in ("completed", "failed", "busy", "no-answer", "canceled"):
            await call_status_queues[parent_sid].put(status)

    return Response(content=str(VoiceResponse()), media_type="text/xml")


# ── Health ─────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}