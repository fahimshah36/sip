import os
import asyncio
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from twilio.jwt.access_token import AccessToken
from twilio.jwt.access_token.grants import VoiceGrant
from twilio.twiml.voice_response import VoiceResponse
from twilio.rest import Client
from fastapi.responses import JSONResponse, Response


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
# VOICE — dials the phone, waits for answer
# answerOnBridge=True means SDK accept fires only
# when the callee actually picks up
# ─────────────────────────────
@app.post("/voice")
async def voice(req: Request):
    form = await req.form()
    to = form.get("To", "")
    print(f"/voice triggered → dialing {to}")
    response = VoiceResponse()
    dial = response.dial(
        caller_id=TWILIO_NUMBER,
        answer_on_bridge=True,
        action=f"{BASE_URL}/dial-complete",
        method="POST",
    )
    dial.number(to)  # NO url= here
    return Response(content=str(response), media_type="text/xml")


# ─────────────────────────────
# DIAL COMPLETE — runs after callee hangs up
# ─────────────────────────────
@app.post("/dial-complete")
async def dial_complete(req: Request):
    form = await req.form()
    dial_status = form.get("DialCallStatus", "")
    print(f"/dial-complete → DialCallStatus: {dial_status}")
    response = VoiceResponse()
    response.say("The call has ended. Goodbye.", voice="alice")
    return Response(content=str(response), media_type="text/xml")


# ─────────────────────────────
# DTMF RECEIVER
# ─────────────────────────────
@app.post("/dtmf")
async def dtmf(req: Request):
    form = await req.form()
    digit    = form.get("Digits", "")
    call_sid = form.get("CallSid", "")
    print(f"DIGIT PRESSED: {digit} | CALL SID: {call_sid}")
    response = VoiceResponse()
    response.say(f"You pressed {digit}.", voice="alice")
    gather = response.gather(
        input="dtmf",
        action=f"{BASE_URL}/dtmf",
        method="POST",
        num_digits=1,
        timeout=30,
        finish_on_key=""
    )
    gather.say("Press another key.", voice="alice")
    return Response(content=str(response), media_type="text/xml")


# ─────────────────────────────
# CALL STATUS
# ─────────────────────────────
@app.post("/status")
async def status(req: Request):
    form = await req.form()
    call_status = form.get("CallStatus", "")
    call_sid    = form.get("CallSid", "")
    duration    = form.get("CallDuration", "0")
    print(f"Status: {call_status.upper()} | SID: {call_sid} | Duration: {duration}s")
    return Response(content="", status_code=200)


# ─────────────────────────────
# HEALTH CHECK
# ─────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok"}


# ─────────────────────────────
# DEBUG
# ─────────────────────────────
@app.get("/test-twilio-reach")
async def test_reach():
    try:
        _, writer = await asyncio.wait_for(
            asyncio.open_connection("chunder.twilio.com", 443),
            timeout=5
        )
        writer.close()
        return {"status": "✅ can reach Twilio"}
    except Exception as e:
        return {"status": f"❌ blocked: {e}"}