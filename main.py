import os
import asyncio
import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from twilio.jwt.access_token import AccessToken
from twilio.jwt.access_token.grants import VoiceGrant
from twilio.twiml.voice_response import VoiceResponse
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
# WEBSOCKET PROXY
# ─────────────────────────────
@app.websocket("/signal-proxy")
async def signal_proxy(browser_ws: WebSocket):
    requested = browser_ws.headers.get("sec-websocket-protocol", "")
    subprotocols = [p.strip() for p in requested.split(",")] if requested else []
    print(f"[proxy] subprotocols requested: {subprotocols}")

    await browser_ws.accept(subprotocol=subprotocols[0] if subprotocols else None)
    print("[proxy] Browser accepted")

    try:
        # websockets v16 API
        async with websockets.connect(
            "wss://chunder.twilio.com/signal",
            additional_headers={
                "Origin": "https://voice.twilio.com",
                "User-Agent": "Mozilla/5.0 TwilioProxy/1.0",
                "Sec-WebSocket-Protocol": ",".join(subprotocols) if subprotocols else "voice",
            },
            open_timeout=10,
        ) as twilio_ws:
            print("[proxy] Connected to Twilio ✅")

            async def browser_to_twilio():
                try:
                    while True:
                        data = await browser_ws.receive_text()
                        await twilio_ws.send(data)
                except Exception as e:
                    print(f"[proxy] browser→twilio closed: {e}")
                    await twilio_ws.close()

            async def twilio_to_browser():
                try:
                    async for message in twilio_ws:
                        await browser_ws.send_text(message)
                except Exception as e:
                    print(f"[proxy] twilio→browser closed: {e}")

            await asyncio.gather(browser_to_twilio(), twilio_to_browser())

    except Exception as e:
        print(f"[proxy] Failed to connect to Twilio: {e}")
    finally:
        print("[proxy] Connection closed")


# ─────────────────────────────
# MAKE CALL
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
# VOICE (TwiML)
# ─────────────────────────────
@app.post("/voice")
async def voice(req: Request):
    print("/voice triggered")
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
    return JSONResponse(content=str(response), media_type="text/xml")


# ─────────────────────────────
# DTMF RECEIVER
# ─────────────────────────────
@app.post("/dtmf")
async def dtmf(req: Request):
    form = await req.form()
    digit    = form.get("Digits", "")
    call_sid = form.get("CallSid", "")
    print("=" * 40)
    print(f"DIGIT PRESSED : {digit}")
    print(f"CALL SID      : {call_sid}")
    print("=" * 40)
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
    return JSONResponse(content=str(response), media_type="text/xml")


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


# ─────────────────────────────
# HEALTH CHECK
# ─────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok"}