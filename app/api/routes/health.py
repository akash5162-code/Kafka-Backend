"""
app/api/routes/health.py
─────────────────────────
Health check endpoint — used by Docker and load balancers
to determine if the service is ready to accept traffic.
"""
from fastapi import APIRouter, Request

from app.core.config import settings
from app.schemas.event import HealthResponse

router = APIRouter(tags=["Health"])


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Health Check",
    description="Returns the current health status of the API and Kafka connectivity.",
)
async def health_check(request: Request) -> HealthResponse:
    kafka_producer = getattr(request.app.state, "kafka_producer", None)
    kafka_ok = kafka_producer.is_connected if kafka_producer else False

    return HealthResponse(
        status="healthy" if kafka_ok else "degraded",
        kafka_connected=kafka_ok,
        app_name=settings.app_name,
        environment=settings.app_env,
    )
