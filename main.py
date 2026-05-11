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


@app.post("/voice")
async def voice(req: Request):
    form = await req.form()
    to = form.get("To", "")
    print(f"/voice triggered → dialing {to}")
    response = VoiceResponse()
    dial = response.dial(caller_id=TWILIO_NUMBER)
    dial.number(to, url=f"{BASE_URL}/connected")
    return Response(content=str(response), media_type="text/xml")  # use Response not JSONResponse


@app.post("/connected")
async def connected(req: Request):
    response = VoiceResponse()
    gather = response.gather(
        input="dtmf",
        action=f"{BASE_URL}/dtmf",
        method="POST",
        num_digits=1,
        timeout=30,
        finish_on_key=""
    )
    gather.say("Hello! Please press any key on your keypad.", voice="alice")
    response.say("No input received. Goodbye!", voice="alice")
    return Response(content=str(response), media_type="text/xml")  # same here


@app.post("/dtmf")
async def dtmf(req: Request):
    form = await req.form()
    digit = form.get("Digits", "")
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
    return Response(content=str(response), media_type="text/xml")  # same here


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
    return JSONResponse(content="", status_code=200)