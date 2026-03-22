"""
main.py
───────
FastAPI application entry point.

Run:
    uvicorn main:app --host 0.0.0.0 --port 8000 --reload

Install URL for Stremio:
    http://localhost:8000/{b64_config}/manifest.json
"""

import logging
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from router import router
from utils.logger import setup_logging
from settings import ADDON_NAME, LOG_LEVEL

setup_logging(LOG_LEVEL)
logger = logging.getLogger(__name__)

# Silence uvicorn access log (hides the long base64 URLs)
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

app = FastAPI(title=ADDON_NAME, docs_url=None, redoc_url=None)

# CORS (Stremio requires wildcard)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "HEAD"],
    allow_headers=["*"],
)

app.include_router(router)

try:
    static_dir = Path(__file__).parent / "static"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
except Exception:
    pass


@app.middleware("http")
async def silence_access_logs(request: Request, call_next):
    # Stremio sends HEAD requests for connectivity checks – return 200 silently
    if request.method == "HEAD":
        from fastapi.responses import Response
        return Response(status_code=200)
    return await call_next(request)


@app.exception_handler(Exception)
async def global_error(request: Request, exc: Exception):
    logger.error("Unhandled error on %s: %s", request.url.path, exc, exc_info=True)
    return JSONResponse({"streams": []}, status_code=200)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)