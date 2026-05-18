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
        incoming_allow=True,
    )
    access_token.add_grant(voice_grant)
    return {"token": access_token.to_jwt()}


# ─────────────────────────────
# VOICE
# No answer_on_bridge — audio connects immediately when callee picks up.
# No url= whisper — nothing to interfere with the bridge.
# Frontend uses SSE in-progress as the live trigger (not SDK accept event).
# ─────────────────────────────
@app.post("/voice")
async def voice(req: Request):
    form = await req.form()
    to         = form.get("To", "")
    parent_sid = form.get("CallSid", "")

    print(f"/voice → dialing {to} | parent: {parent_sid}")

    # Ensure queue exists before any status callbacks arrive
    if parent_sid:
        call_status_queues[parent_sid]

    response = VoiceResponse()
    dial = Dial(
        caller_id=TWILIO_NUMBER,
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


# ─────────────────────────────
# CHILD STATUS
# Forwards call lifecycle events to the browser via SSE queue.
# in-progress  → browser shows "Live" and opens audio
# completed    → browser ends the call screen
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
# DTMF
# Called by Twilio when the callee presses a digit.
# The digit is pushed to the SSE queue so the browser receives it instantly.
# A fresh Gather is returned to keep listening for more digits.
# ─────────────────────────────
@app.post("/dtmf")
async def dtmf(req: Request):
    parent_sid = req.query_params.get("parent", "")
    form       = await req.form()
    digit      = form.get("Digits", "")

    print(f"[dtmf] digit={digit!r} | parent={parent_sid}")

    if digit and parent_sid and parent_sid in call_status_queues:
        await call_status_queues[parent_sid].put(f"dtmf:{digit}")

    # Return a fresh Gather so the callee can press more digits
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
    return Response(content=str(response), media_type="text/xml")


# ─────────────────────────────
# CHILD TWIML  (whisper URL — runs on callee leg in parallel with bridge)
# Returns a Gather so Twilio listens for DTMF on the callee's phone.
# timeout=30 + redirect loop avoids long-running HTTP connections being
# killed by the hosting platform (e.g. Render free tier 30s limit).
# ─────────────────────────────
@app.post("/child-twiml")
async def child_twiml(req: Request):
    parent_sid = req.query_params.get("parent", "")
    response = VoiceResponse()
    gather = Gather(
        input="dtmf",
        action=f"{BASE_URL}/dtmf?parent={parent_sid}",
        method="POST",
        num_digits=1,
        timeout=25,         # short enough to stay under Render's 30s limit
        finish_on_key="",
    )
    response.append(gather)
    # Loop back whether or not a digit was pressed
    response.redirect(
        f"{BASE_URL}/child-twiml?parent={parent_sid}", method="POST"
    )
    return Response(content=str(response), media_type="text/xml")


# ─────────────────────────────
# SSE  — browser subscribes to real-time call events + DTMF digits
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
        },
    )


# ─────────────────────────────
# DIAL COMPLETE
# ─────────────────────────────
@app.post("/dial-complete")
async def dial_complete(req: Request):
    form        = await req.form()
    dial_status = form.get("DialCallStatus", "")
    parent_sid  = form.get("CallSid", "")
    print(f"/dial-complete → DialCallStatus: {dial_status} | parent: {parent_sid}")

    # Push terminal event so SSE stream closes cleanly
    if parent_sid and parent_sid in call_status_queues:
        status_map = {
            "completed": "completed",
            "busy":      "busy",
            "no-answer": "no-answer",
            "failed":    "failed",
            "canceled":  "canceled",
        }
        mapped = status_map.get(dial_status)
        if mapped:
            await call_status_queues[parent_sid].put(mapped)

    return Response(content=str(VoiceResponse()), media_type="text/xml")


# ─────────────────────────────
# PARENT CALL STATUS (fallback)
# ─────────────────────────────
@app.post("/status")
async def status(req: Request):
    form        = await req.form()
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