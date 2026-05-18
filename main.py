import os
import asyncio
import json
import audioop
import struct
import math
import base64
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


# ── DTMF Detection ─────────────────────────────────────────────────────────────

DTMF_FREQS = {
    '1': (697, 1209), '2': (697, 1336), '3': (697, 1477),
    '4': (770, 1209), '5': (770, 1336), '6': (770, 1477),
    '7': (852, 1209), '8': (852, 1336), '9': (852, 1477),
    '*': (941, 1209), '0': (941, 1336), '#': (941, 1477),
}

ALL_FREQS   = list({f for pair in DTMF_FREQS.values() for f in pair})
SAMPLE_RATE = 8000
FRAME_SIZE  = 160  # 20ms at 8kHz


def goertzel(samples: list[float], target_freq: float, sample_rate: int) -> float:
    n     = len(samples)
    k     = int(0.5 + n * target_freq / sample_rate)
    omega = 2 * math.pi * k / n
    coeff = 2 * math.cos(omega)
    s_prev, s_prev2 = 0.0, 0.0
    for sample in samples:
        s       = sample + coeff * s_prev - s_prev2
        s_prev2 = s_prev
        s_prev  = s
    return s_prev2 ** 2 + s_prev ** 2 - coeff * s_prev * s_prev2


def detect_dtmf(pcm_samples: list[float]) -> str | None:
    powers = {f: goertzel(pcm_samples, f, SAMPLE_RATE) for f in ALL_FREQS}
    total  = sum(powers.values())
    if total < 1e6:
        return None
    low_f  = max((f for f in ALL_FREQS if f <= 941),  key=lambda f: powers[f])
    high_f = max((f for f in ALL_FREQS if f >= 1209), key=lambda f: powers[f])
    low_p  = powers[low_f]  / total
    high_p = powers[high_f] / total
    if low_p < 0.15 or high_p < 0.15:
        return None
    for digit, (lf, hf) in DTMF_FREQS.items():
        if lf == low_f and hf == high_f:
            return digit
    return None


class DtmfDetector:
    def __init__(self):
        self.buffer: list[float] = []
        self.last_digit: str | None = None
        self.confirm_count  = 0
        self.silence_count  = 0
        self.CONFIRM_FRAMES = 3  # ~60ms of consistent tone
        self.SILENCE_RESET  = 2

    def feed(self, mulaw_b64: str) -> str | None:
        raw     = base64.b64decode(mulaw_b64)
        pcm_raw = audioop.ulaw2lin(raw, 2)
        samples = [
            struct.unpack_from('<h', pcm_raw, i)[0]
            for i in range(0, len(pcm_raw), 2)
        ]
        self.buffer.extend(samples)

        result = None
        while len(self.buffer) >= FRAME_SIZE:
            frame       = self.buffer[:FRAME_SIZE]
            self.buffer = self.buffer[FRAME_SIZE:]
            digit       = detect_dtmf(frame)

            if digit is None:
                self.silence_count += 1
                if self.silence_count >= self.SILENCE_RESET:
                    self.last_digit    = None
                    self.confirm_count = 0
            else:
                self.silence_count = 0
                if digit == self.last_digit:
                    self.confirm_count += 1
                    if self.confirm_count == self.CONFIRM_FRAMES:
                        result = digit
                else:
                    self.last_digit    = digit
                    self.confirm_count = 1

        return result


dtmf_detectors: dict[str, DtmfDetector] = defaultdict(DtmfDetector)


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
        status_callback_event="initiated ringing answered completed",
    ))
    response.append(dial)

    return Response(content=str(response), media_type="text/xml")


# ── Media Streams WebSocket ────────────────────────────────────────────────────

@app.websocket("/media-stream/{call_sid}")
async def media_stream(websocket: WebSocket, call_sid: str):
    await websocket.accept()
    detector = dtmf_detectors[call_sid]
    try:
        while True:
            data = await websocket.receive_text()
            msg  = json.loads(data)
            event = msg.get("event")

            if event == "media":
                media = msg.get("media", {})
                if media.get("track") == "outbound":
                    digit = detector.feed(media.get("payload", ""))
                    if digit:
                        print(f"[dtmf] detected={digit!r}  call_sid={call_sid}")
                        asyncio.create_task(
                            call_status_queues[call_sid].put(f"dtmf:{digit}")
                        )
            else:
                print(f"[media-stream] event={event}  call_sid={call_sid}")

    except WebSocketDisconnect:
        dtmf_detectors.pop(call_sid, None)
    except Exception as e:
        print(f"[media-stream] error: {e}")
        dtmf_detectors.pop(call_sid, None)


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