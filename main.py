import os
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from twilio.jwt.access_token import AccessToken
from twilio.jwt.access_token.grants import VoiceGrant
from twilio.twiml.voice_response import VoiceResponse, Dial, Number
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
# MAKE CALL (kept for reference, not used by browser SDK flow)
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
# VOICE — dials the phone
# answer_on_bridge=True means SDK accept fires only
# when the callee actually picks up
# ─────────────────────────────
@app.post("/voice")
async def voice(req: Request):
    form = await req.form()
    to = form.get("To", "")
    print(f"/voice triggered → dialing {to}")
    response = VoiceResponse()
    dial = Dial(
        caller_id=TWILIO_NUMBER,
        answer_on_bridge=True,
        action=f"{BASE_URL}/dial-complete",
        method="POST",
    )
    dial.append(Number(to))
    response.append(dial)
    return Response(content=str(response), media_type="text/xml")


# ─────────────────────────────
# DIAL COMPLETE — runs after callee hangs up
# ─────────────────────────────
@app.post("/dial-complete")
async def dial_complete(req: Request):
    form = await req.form()
    dial_status = form.get("DialCallStatus", "")
    print(f"/dial-complete → DialCallStatus: {dial_status}")
    return Response(content=str(VoiceResponse()), media_type="text/xml")


# ─────────────────────────────
# CALL STATUS — for logging
# ─────────────────────────────
@app.post("/status")
async def status(req: Request):
    form = await req.form()
    call_status = form.get("CallStatus", "")
    call_sid    = form.get("CallSid", "")
    duration    = form.get("CallDuration", "0")
    print(f"[status] {call_status.upper()} | SID: {call_sid} | Duration: {duration}s")
    return Response(content="", status_code=200)


# ─────────────────────────────
# HEALTH CHECK
# ─────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok"}