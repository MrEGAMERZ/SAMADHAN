"""
VANTA - Governance Intelligence Platform (Backend Entry Point)
============================================================
This file initializes the FastAPI application, sets up the SQLite/MongoDB Mock ORM connection, 
mounts all API routers, configures the WebSocket Connection Manager for real-time telemetry, 
and serves static files for the frontend deployment.
"""

import os
from dotenv import load_dotenv
load_dotenv()
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

import asyncio
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from typing import List
import json

from database import engine, Base, SessionLocal
from seed_data import seed_db

# Import routers
from routes import auth, complaints, officials, map, resolution, escalation, projects, upload, transparency, tenders

Base.metadata.create_all(bind=engine)
try:
    seed_db()
except Exception as e:
    print(f"[WARN] seed_db() failed (DB may not be ready yet): {e}")

app = FastAPI(
    title="SAMADHAN — Governance Intelligence Platform API",
    description="Engineered in the dark. Built to last.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response, JSONResponse
import time

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        response.headers["Content-Security-Policy"] = "default-src 'self' 'unsafe-inline' 'unsafe-eval' data: blob: *;"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        return response

class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app):
        super().__init__(app)
        self.rate_limits = {}
        self.limit = 100  # requests
        self.window = 60  # seconds

    async def dispatch(self, request, call_next):
        client_ip = request.client.host if request.client else "127.0.0.1"
        current_time = time.time()
        
        # Clean up old entries periodically or just for this IP
        if client_ip not in self.rate_limits:
            self.rate_limits[client_ip] = []
            
        # Filter timestamps within the current window
        self.rate_limits[client_ip] = [t for t in self.rate_limits[client_ip] if current_time - t < self.window]
        
        if len(self.rate_limits[client_ip]) >= self.limit:
            return JSONResponse(status_code=429, content={"detail": "Too Many Requests"})
            
        self.rate_limits[client_ip].append(current_time)
        return await call_next(request)

app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(RateLimitMiddleware)

# Mount routes
app.include_router(auth.router)
app.include_router(complaints.router)
app.include_router(officials.router)
app.include_router(map.router)
app.include_router(resolution.router)
app.include_router(escalation.router)
app.include_router(projects.router)
app.include_router(upload.router)
app.include_router(transparency.router)
app.include_router(tenders.router)

from fastapi.staticfiles import StaticFiles
uploads_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "public", "uploads")
if not os.path.exists(uploads_path):
    os.makedirs(uploads_path, exist_ok=True)
app.mount("/public/uploads", StaticFiles(directory=uploads_path), name="uploads")


class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        stale: List[WebSocket] = []
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception:
                stale.append(connection)
        for ws in stale:
            self.active_connections.remove(ws)


manager = ConnectionManager()


@app.websocket("/ws/map")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)


async def notify_clients(event_type: str, data: dict):
    payload = json.dumps({"event": event_type, "data": data})
    await manager.broadcast(payload)


@app.get("/api/health")
def health_check():
    return {"status": "healthy", "platform": "SAMADHAN", "epoch": 2026}


app.state.notify_clients = notify_clients

_ESCALATION_RUNNING = False
_AUTO_VERIFY_RUNNING = False


@app.on_event("startup")
async def start_background_sweeps():
    from services.escalation import check_and_escalate_overdue_complaints
    from datetime import timedelta

    async def sweep_loop():
        global _ESCALATION_RUNNING
        await asyncio.sleep(10)
        while True:
            if not _ESCALATION_RUNNING:
                _ESCALATION_RUNNING = True
                try:
                    db_session = SessionLocal()
                    try:
                        count = check_and_escalate_overdue_complaints(db_session)
                        if count > 0:
                            await notify_clients(
                                "ESCALATION_SWEEP", {"count": count}
                            )
                    finally:
                        db_session.close()
                except Exception as e:
                    print(f"[Escalation Sweep Error] {e}")
                finally:
                    _ESCALATION_RUNNING = False
            await asyncio.sleep(60)

    async def auto_verify_loop():
        global _AUTO_VERIFY_RUNNING
        from models.models import Complaint, Official
        await asyncio.sleep(30)
        while True:
            if not _AUTO_VERIFY_RUNNING:
                _AUTO_VERIFY_RUNNING = True
                try:
                    db_session = SessionLocal()
                    try:
                        cutoff = datetime.utcnow() - timedelta(hours=48)
                        pending = db_session.query(Complaint).filter(
                            Complaint.status == "PENDING_VERIFICATION",
                            Complaint.resolved_at != None,
                            Complaint.resolved_at < cutoff,
                        ).all()
                        count = 0
                        for complaint in pending:
                            complaint.status = "RESOLVED"
                            complaint.resolution_status = "AUTO_VERIFIED"
                            complaint.verified_at = datetime.utcnow()
                            complaint.verification_yes = complaint.verification_yes or 0
                            if complaint.assigned_to:
                                official = db_session.query(Official).filter(
                                    Official.id == complaint.assigned_to
                                ).first()
                                if official:
                                    official.complaints_resolved += 1
                                    official.resolution_rate = round(
                                        (official.complaints_resolved / max(official.complaints_assigned, 1)) * 100, 1
                                    )
                            count += 1
                        if count > 0:
                            db_session.commit()
                            await notify_clients(
                                "AUTO_VERIFY_SWEEP", {"count": count}
                            )
                    finally:
                        db_session.close()
                except Exception as e:
                    print(f"[Auto-Verify Sweep Error] {e}")
                finally:
                    _AUTO_VERIFY_RUNNING = False
            await asyncio.sleep(120)

    asyncio.create_task(sweep_loop())
    asyncio.create_task(auto_verify_loop())


dist_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dist")
if os.path.exists(dist_path):
    assets_path = os.path.join(dist_path, "assets")
    if os.path.exists(assets_path):
        app.mount("/assets", StaticFiles(directory=assets_path), name="assets")

    @app.get("/{filename:path}")
    async def serve_static_or_spa(filename: str):
        if filename.startswith("api") or filename.startswith("ws"):
            return {"detail": "Not Found"}

        file_path = os.path.join(dist_path, filename)
        if filename and os.path.exists(file_path) and os.path.isfile(file_path):
            return FileResponse(file_path)

        return FileResponse(os.path.join(dist_path, "index.html"))


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
