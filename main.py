"""
Reference Twilio voice backend (fixes for browser ↔ PSTN + DTMF).

Key fixes vs a broken setup:
1. child-twiml must NOT return an empty <Gather> — that breaks voice after answer.
   Use <Gather> with <Pause> inside so the bridge stays up while listening for DTMF.
2. SSE pushes raw Twilio CallStatus: initiated, ringing, in-progress, completed, …
   Frontend should go Live on "in-progress" (callee answered), not on browser "accept".
"""

import os
import asyncio
from collections import defaultdict

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, StreamingResponse
from twilio.jwt.access_token import AccessToken
from twilio.jwt.access_token.grants import VoiceGrant
from twilio.twiml.voice_response import VoiceResponse, Dial, Number, Gather, Pause, Redirect
from twilio.rest import Client

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

ACCOUNT_SID = os.environ["ACCOUNT_SID"]
AUTH_TOKEN = os.environ["AUTH_TOKEN"]
API_KEY = os.environ["API_KEY"]
API_SECRET = os.environ["API_SECRET"]
TWIML_APP_SID = os.environ["TWIML_APP_SID"]
TWILIO_NUMBER = os.environ["TWILIO_NUMBER"]
BASE_URL = os.environ["BASE_URL"]

client = Client(ACCOUNT_SID, AUTH_TOKEN)
call_status_queues: dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)


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
        answer_on_bridge=True,
        action=f"{BASE_URL}/dial-complete",
        method="POST",
    )
    number = Number(
        to,
        # ← REMOVE the url= entirely. It was holding the bridge hostage.
        status_callback=f"{BASE_URL}/child-status",
        status_callback_method="POST",
        status_callback_event="initiated ringing answered completed",
    )
    dial.append(number)
    response.append(dial)
    return Response(content=str(response), media_type="text/xml")


@app.post("/child-twiml")
async def child_twiml(req: Request):
    """Runs when callee answers. Gather + Pause keeps the voice bridge open for DTMF."""
    parent_sid = req.query_params.get("parent", "")
    response = VoiceResponse()
    gather = Gather(
        input="dtmf",
        action=f"{BASE_URL}/dtmf?parent={parent_sid}",
        method="POST",
        num_digits=1,
        timeout=60,
    )
    gather.append(Pause(length=3600))
    response.append(gather)
    return Response(content=str(response), media_type="text/xml")


@app.post("/child-status")
async def child_status(req: Request):
    form = await req.form()
    call_status = form.get("CallStatus", "")
    parent_sid = form.get("ParentCallSid", "")
    child_sid = form.get("CallSid", "")
    print(f"child-status: {call_status} for parent {parent_sid}")

    if parent_sid and parent_sid in call_status_queues:
        await call_status_queues[parent_sid].put(call_status)

    # ← When child answers (bridge formed), inject Gather onto child leg
    if call_status == "in-progress" and child_sid:
        asyncio.create_task(inject_dtmf_gather(child_sid, parent_sid))

    return Response(content="", status_code=200)


@app.post("/dtmf")
async def dtmf(req: Request):
    parent_sid = req.query_params.get("parent", "")
    form = await req.form()
    digit = form.get("Digits", "")

    if parent_sid and parent_sid in call_status_queues:
        await call_status_queues[parent_sid].put(f"dtmf:{digit}")

    response = VoiceResponse()
    gather = Gather(
        input="dtmf",
        action=f"{BASE_URL}/dtmf?parent={parent_sid}",
        method="POST",
        num_digits=1,
        timeout=60,
    )
    gather.append(Pause(length=3600))
    response.append(gather)
    return Response(content=str(response), media_type="text/xml")


@app.get("/call-events/{call_sid}")
async def call_events(call_sid: str):
    queue = call_status_queues[call_sid]

    async def stream():
        try:
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=25)
                    yield f"data: {event}\n\n"
                    if event in (
                        "completed",
                        "failed",
                        "busy",
                        "no-answer",
                        "canceled",
                    ):
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


@app.post("/dial-complete")
async def dial_complete(req: Request):
    form = await req.form()
    # dial-complete fires when the bridged call ends — nothing to do
    return Response(content=str(VoiceResponse()), media_type="text/xml")

async def inject_dtmf_gather(child_sid: str, parent_sid: str):
    """Modify the live child leg to listen for DTMF indefinitely."""
    import asyncio
    loop = asyncio.get_event_loop()
    
    def _update():
        response = VoiceResponse()
        gather = Gather(
            input="dtmf",
            action=f"{BASE_URL}/dtmf?parent={parent_sid}",
            method="POST",
            num_digits=1,
            timeout=60,        # wait 60s for a digit, then re-gather
            action_on_empty_result=True,  # re-hit action even with no input
        )
        gather.append(Pause(length=3600))
        response.append(gather)
        # If gather times out with no digit, loop back
        response.append(Redirect(f"{BASE_URL}/regather?parent={parent_sid}&child={child_sid}"))
        
        client.calls(child_sid).update(twiml=str(response))
    
    await loop.run_in_executor(None, _update)


@app.post("/regather")
async def regather(req: Request):
    """Called when Gather times out — reinject to keep listening."""
    parent_sid = req.query_params.get("parent", "")
    child_sid = req.query_params.get("child", "")
    
    if child_sid:
        asyncio.create_task(inject_dtmf_gather(child_sid, parent_sid))
    
    return Response(content=str(VoiceResponse()), media_type="text/xml")


@app.post("/status")
async def status(req: Request):
    form = await req.form()
    call_status = form.get("CallStatus", "")
    call_sid = form.get("CallSid", "")

    if call_sid in call_status_queues and call_status in ("completed", "failed"):
        await call_status_queues[call_sid].put(call_status)

    return Response(content="", status_code=200)


@app.get("/health")
def health():
    return {"status": "ok"}
