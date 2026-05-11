import os
import asyncio
import threading
import websockets
from flask import Flask, jsonify, request
from twilio.jwt.access_token import AccessToken
from twilio.jwt.access_token.grants import VoiceGrant
from twilio.twiml.voice_response import VoiceResponse
from twilio.rest import Client
from flask_cors import CORS
from flask_sock import Sock

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})
sock = Sock(app)

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
@app.route("/token")
def token():
    access_token = AccessToken(
        ACCOUNT_SID,
        API_KEY,
        API_SECRET,
        identity="visitor"
    )
    voice_grant = VoiceGrant(
        outgoing_application_sid=TWIML_APP_SID,
        incoming_allow=True
    )
    access_token.add_grant(voice_grant)
    return jsonify({"token": access_token.to_jwt()})


# ─────────────────────────────
# WEBSOCKET PROXY
# ─────────────────────────────
@sock.route("/signal-proxy")
def signal_proxy(ws):
    requested = request.headers.get("Sec-WebSocket-Protocol", "")
    subprotocols = [p.strip() for p in requested.split(",")] if requested else []
    print(f"[proxy] Browser requested subprotocols: {subprotocols}")

    async def run():
        try:
            connect_kwargs = {
                "extra_headers": {
                    "User-Agent": "Mozilla/5.0 TwilioProxy/1.0",
                    "Origin": "https://voice.twilio.com",
                },
            }
            if subprotocols:
                connect_kwargs["subprotocols"] = subprotocols

            async with websockets.connect(
                "wss://chunder.twilio.com/signal",
                **connect_kwargs
            ) as twilio_ws:
                print("[proxy] Connected to Twilio successfully")

                async def browser_to_twilio():
                    while True:
                        data = ws.receive()
                        if data is None:
                            print("[proxy] Browser disconnected")
                            break
                        await twilio_ws.send(data)

                async def twilio_to_browser():
                    async for message in twilio_ws:
                        ws.send(message)

                await asyncio.gather(
                    browser_to_twilio(),
                    twilio_to_browser()
                )
        except Exception as e:
            print(f"[proxy error] {e}")

    loop = asyncio.new_event_loop()

    def run_loop():
        asyncio.set_event_loop(loop)
        loop.run_until_complete(run())

    t = threading.Thread(target=run_loop)
    t.start()
    t.join()


# ─────────────────────────────
# MAKE CALL
# ─────────────────────────────
@app.route("/make-call", methods=["POST"])
def make_call():
    to = request.json.get("to")
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
    return jsonify({"call_sid": call.sid, "status": call.status})


# ─────────────────────────────
# VOICE (TwiML)
# ─────────────────────────────
@app.route("/voice", methods=["POST"])
def voice():
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
    return str(response)


# ─────────────────────────────
# DTMF RECEIVER
# ─────────────────────────────
@app.route("/dtmf", methods=["POST"])
def dtmf():
    digit    = request.form.get("Digits", "")
    call_sid = request.form.get("CallSid", "")
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
    return str(response)


# ─────────────────────────────
# CALL STATUS
# ─────────────────────────────
@app.route("/status", methods=["POST"])
def status():
    call_status = request.form.get("CallStatus", "")
    call_sid    = request.form.get("CallSid", "")
    duration    = request.form.get("CallDuration", "0")
    print(f"Status: {call_status.upper()} | SID: {call_sid} | Duration: {duration}s")
    return "", 200


# ─────────────────────────────
# HEALTH CHECK
# ─────────────────────────────
@app.route("/health")
def health():
    return jsonify({"status": "ok"}), 200


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)