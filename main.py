import os
import asyncio
import json
from collections import defaultdict

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, StreamingResponse
from twilio.jwt.access_token import AccessToken
from twilio.jwt.access_token.grants import VoiceGrant
from twilio.twiml.voice_response import VoiceResponse, Dial, Number, Start, Stream

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

call_status_queues: dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)


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

    # <Start><Stream> is non-blocking — falls through to <Dial> immediately.
    # track="both_tracks" is required to capture DTMF from the callee (outbound track).
    start = Start()
    start.stream(
        url=f"{ws_base}/media-stream/{parent_sid}",
        track="both_tracks",
    )
    response.append(start)

    # Dial — unchanged
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
        status_callback_event="initiated ringing answered completed",
    ))
    response.append(dial)

    return Response(content=str(response), media_type="text/xml")


# ── Media Streams WebSocket ────────────────────────────────────────────────────

@app.websocket("/media-stream/{call_sid}")
async def media_stream(websocket: WebSocket, call_sid: str):
    await websocket.accept()
    try:
        while True:
            data = await websocket.receive_text()
            msg  = json.loads(data)

            if msg.get("event") == "dtmf":
                digit = msg.get("dtmf", {}).get("digit", "")
                print(f"[dtmf] digit={digit}  call_sid={call_sid}")
                if digit and call_sid in call_status_queues:
                    await call_status_queues[call_sid].put(f"dtmf:{digit}")

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"[media-stream] error: {e}")


# ── Child leg status callbacks ─────────────────────────────────────────────────

@app.post("/child-status")
async def child_status(req: Request):
    form        = await req.form()
    call_status = form.get("CallStatus", "")
    parent_sid  = form.get("ParentCallSid", "")
    print(f"[child-status] {call_status}  parent={parent_sid}")

    if parent_sid and parent_sid in call_status_queues:
        await call_status_queues[parent_sid].put(call_status)

    return Response(content="", status_code=200)


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