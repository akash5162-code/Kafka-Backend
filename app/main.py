"""
app/main.py
────────────
FastAPI application entry point.

Lifespan Context Manager:
  - ON STARTUP:  Starts the Kafka producer and stores it on app.state.
  - ON SHUTDOWN: Gracefully stops the producer (flushes in-flight messages).

This pattern ensures:
  1. No Kafka connections are left open after the server exits.
  2. The producer is available as a singleton across ALL requests without
     the overhead of creating a new connection per request.
"""
import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.routes import events, health
from app.core.config import settings
from app.services.kafka_producer import KafkaProducerService

# ── Logging Setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── Lifespan ──────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages the lifecycle of background resources.
    Code BEFORE yield = startup.
    Code AFTER  yield = shutdown.
    """
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logger.info("  🚀 Starting %s [%s]", settings.app_name, settings.app_env)
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    # ── Startup ───────────────────────────────────────────
    producer = KafkaProducerService()
    await producer.start()
    app.state.kafka_producer = producer

    logger.info("✅ All services initialized. Ready to accept traffic.")

    yield  # ← App is running here

    # ── Shutdown ──────────────────────────────────────────
    logger.info("🛑 Shutting down — flushing Kafka producer...")
    await app.state.kafka_producer.stop()
    logger.info("✅ Graceful shutdown complete.")


# ── App Factory ───────────────────────────────────────────────────────────────
app = FastAPI(
    title=settings.app_name,
    description=(
        "A production-grade, async, event-driven backend that ingests "
        "high-throughput traffic and publishes events to Apache Kafka. "
        "Built with FastAPI + aiokafka."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# ── Middleware ─────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers ───────────────────────────────────────────────────────────────────
app.include_router(health.router, prefix=settings.api_prefix)
app.include_router(events.router, prefix=settings.api_prefix)


# ── Root ──────────────────────────────────────────────────────────────────────
@app.get("/", include_in_schema=False)
async def root():
    return JSONResponse({
        "service": settings.app_name,
        "version": "1.0.0",
        "docs": "/docs",
        "health": f"{settings.api_prefix}/health",
    })


# ── Dev Runner ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host=settings.app_host,
        port=settings.app_port,
        reload=settings.app_env == "development",
        log_level="info",
    )
