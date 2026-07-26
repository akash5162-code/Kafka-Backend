"""
app/api/dependencies.py
────────────────────────
FastAPI dependency injection for the Kafka producer.
Injects the shared producer singleton into route handlers.
"""
from fastapi import Request

from app.services.kafka_producer import KafkaProducerService


def get_kafka_producer(request: Request) -> KafkaProducerService:
    """
    Retrieve the Kafka producer stored on the FastAPI app state.
    Injected via: Depends(get_kafka_producer)
    """
    return request.app.state.kafka_producer
