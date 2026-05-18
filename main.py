import os
import asyncio
from collections import defaultdict
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, StreamingResponse
from twilio.jwt.access_token import AccessToken
from twilio.jwt.access_token.grants import VoiceGrant
from twilio.twiml.voice_response import VoiceResponse, Dial, Number, Gather
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

client = Client(ACCOUNT_SID, AUTH_TOKEN)

# In-memory queues for real-time push via SSE
# key = parent CallSid, value = asyncio.Queue of event strings
call_status_queues: dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)


# ─────────────────────────────
# TOKEN
# ─────────────────────────────
@app.get("/token")
def token():
    access_token = AccessToken(
        ACCOUNT_SID, API_KEY, API_SECRET, identity="visitor"
    )
    voice_grant = VoiceGrant(
        outgoing_application_sid=TWIML_APP_SID,
        incoming_allow=True
    )
    access_token.add_grant(voice_grant)
    return {"token": access_token.to_jwt()}


# ─────────────────────────────
# MAKE CALL (kept for reference)
# ─────────────────────────────
@app.post("/make-call")
async def make_call(req: Request):
    body = await req.json()
    to = body.get("to")
    print(f"Calling {to}...")
    call = client.calls.create(
        to=to,
        from_=TWILIO_NUMBER,
        url=f"{BASE_URL}/voice",
        status_callback=f"{BASE_URL}/status",
        status_callback_method="POST",
        status_callback_event=["initiated", "ringing", "answered", "completed"],
    )
    print(f"Call SID: {call.sid}")
    return {"call_sid": call.sid, "status": call.status}


# ─────────────────────────────
# VOICE — dials the phone + starts DTMF gather loop
# ─────────────────────────────
@app.post("/voice")
async def voice(req: Request):
    form = await req.form()
    to = form.get("To", "")
    parent_sid = form.get("CallSid", "")

    if parent_sid:
        call_status_queues[parent_sid]

    response = VoiceResponse()
    dial = Dial(
        caller_id=TWILIO_NUMBER,
        # answer_on_bridge=True,
        action=f"{BASE_URL}/dial-complete",
        method="POST",
    )
    dial.append(Number(
        to,
        status_callback=f"{BASE_URL}/child-status",
        status_callback_method="POST",
        status_callback_event="initiated ringing answered completed",
        url=f"{BASE_URL}/child-twiml?parent={parent_sid}",
        method="POST",
    ))
    response.append(dial)
    return Response(content=str(response), media_type="text/xml")

@app.post("/child-twiml")
async def child_twiml(req: Request):
    parent_sid = req.query_params.get("parent", "")
    response = VoiceResponse()
    # Gather runs on child leg DURING the bridge (answer_on_bridge=True means
    # bridge starts when child answers, whisper runs concurrently on child leg)
    gather = Gather(
        input="dtmf",
        action=f"{BASE_URL}/dtmf?parent={parent_sid}",
        method="POST",
        num_digits=1,
        timeout=3600,
        finish_on_key="",
    )
    response.append(gather)
    # After gather completes (digit pressed), loop back to keep listening
    response.redirect(f"{BASE_URL}/child-twiml?parent={parent_sid}", method="POST")
    return Response(content=str(response), media_type="text/xml")


# ─────────────────────────────
# CHILD CALL STATUS — phone leg events
# ─────────────────────────────
@app.post("/child-status")
async def child_status(req: Request):
    form = await req.form()
    call_status = form.get("CallStatus", "")
    child_sid   = form.get("CallSid", "")
    parent_sid  = form.get("ParentCallSid", "")
    print(f"[child-status] {call_status.upper()} | child: {child_sid} | parent: {parent_sid}")

    if parent_sid and parent_sid in call_status_queues:
        await call_status_queues[parent_sid].put(call_status)

    return Response(content="", status_code=200)

# ─────────────────────────────
# DTMF — callee pressed a key, push to browser via SSE
# then loop back to gather more digits
# ─────────────────────────────
@app.post("/dtmf")
async def dtmf(req: Request):
    parent_sid = req.query_params.get("parent", "")
    form = await req.form()
    digit = form.get("Digits", "")
    print(f"[dtmf] digit={digit!r} | parent={parent_sid}")

    if digit and parent_sid and parent_sid in call_status_queues:
        await call_status_queues[parent_sid].put(f"dtmf:{digit}")

    # Loop back — keep listening for more digits
    response = VoiceResponse()
    gather = Gather(
        input="dtmf",
        action=f"{BASE_URL}/dtmf?parent={parent_sid}",
        method="POST",
        num_digits=1,
        timeout=3600,
        finish_on_key="",
    )
    response.append(gather)
    response.redirect(f"{BASE_URL}/child-twiml?parent={parent_sid}", method="POST")
    return Response(content=str(response), media_type="text/xml")


# ─────────────────────────────
# SSE — browser subscribes to real-time call events + DTMF
# ─────────────────────────────
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
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        }
    )


# ─────────────────────────────
# DIAL COMPLETE
# ─────────────────────────────
@app.post("/dial-complete")
async def dial_complete(req: Request):
    form = await req.form()
    dial_status = form.get("DialCallStatus", "")
    print(f"/dial-complete → DialCallStatus: {dial_status}")
    return Response(content=str(VoiceResponse()), media_type="text/xml")


# ─────────────────────────────
# PARENT CALL STATUS
# ─────────────────────────────
@app.post("/status")
async def status(req: Request):
    form = await req.form()
    call_status = form.get("CallStatus", "")
    call_sid    = form.get("CallSid", "")
    duration    = form.get("CallDuration", "0")
    print(f"[status] {call_status.upper()} | SID: {call_sid} | Duration: {duration}s")

    if call_sid in call_status_queues and call_status in ("completed", "failed"):
        await call_status_queues[call_sid].put(call_status)

    return Response(content="", status_code=200)


# ─────────────────────────────
# HEALTH CHECK
# ─────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok"}