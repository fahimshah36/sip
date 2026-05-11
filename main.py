import os
import asyncio
import threading
import websockets
import websockets.server
from flask import Flask, jsonify, request
from twilio.jwt.access_token import AccessToken
from twilio.jwt.access_token.grants import VoiceGrant
from twilio.twiml.voice_response import VoiceResponse
from twilio.rest import Client
from flask_cors import CORS

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

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


# ─────────────────────────────
# WEBSOCKET PROXY SERVER
# Runs on port 8765 alongside Flask
# ─────────────────────────────
async def proxy_handler(browser_ws):
    # Forward whatever subprotocol the browser requested
    subprotocols = list(browser_ws.subprotocols) if browser_ws.subprotocols else []
    print(f"[proxy] New connection, subprotocols: {subprotocols}")

    try:
        async with websockets.connect(
            "wss://chunder.twilio.com/signal",
            subprotocols=subprotocols or None,
            extra_headers={
                "Origin": "https://voice.twilio.com",
                "User-Agent": "Mozilla/5.0 TwilioProxy/1.0",
            }
        ) as twilio_ws:
            print("[proxy] Connected to Twilio ✅")

            async def browser_to_twilio():
                try:
                    async for message in browser_ws:
                        await twilio_ws.send(message)
                except Exception as e:
                    print(f"[proxy] browser→twilio error: {e}")

            async def twilio_to_browser():
                try:
                    async for message in twilio_ws:
                        await browser_ws.send(message)
                except Exception as e:
                    print(f"[proxy] twilio→browser error: {e}")

            await asyncio.gather(browser_to_twilio(), twilio_to_browser())

    except Exception as e:
        print(f"[proxy] Connection error: {e}")


def start_proxy():
    port = int(os.environ.get("WS_PORT", 8765))
    print(f"[proxy] WebSocket proxy starting on port {port}")

    async def serve():
        async with websockets.serve(
            proxy_handler,
            "0.0.0.0",
            port,
            subprotocols=["voice"],  # accept voice subprotocol
        ):
            print(f"[proxy] Listening on ws://0.0.0.0:{port}")
            await asyncio.Future()  # run forever

    asyncio.run(serve())


# Start proxy in background thread when app loads
proxy_thread = threading.Thread(target=start_proxy, daemon=True)
proxy_thread.start()


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=5000)