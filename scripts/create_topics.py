"""
scripts/create_topics.py
─────────────────────────
One-time setup script: creates all required Kafka topics with proper
partition counts and replication factors before the app starts.

Run once after Kafka boots:
    python -m scripts.create_topics

In Docker: this runs automatically before the API container starts.
"""
import logging
import sys
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from app.core.config import settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# ── Topic Config ───────────────────────────────────────────────────────────────
# Partitions: higher = more parallelism for consumers
# Replication factor: set to 1 for local dev (increase in prod)
TOPIC_DEFINITIONS = [
    NewTopic(name=settings.kafka_topic_user_actions,  num_partitions=3, replication_factor=1),
    NewTopic(name=settings.kafka_topic_orders,        num_partitions=3, replication_factor=1),
    NewTopic(name=settings.kafka_topic_transactions,  num_partitions=3, replication_factor=1),
    NewTopic(name=settings.kafka_topic_system_logs,   num_partitions=1, replication_factor=1),
    # Dead-letter queues (DLQs) for failed messages
    NewTopic(name=f"{settings.kafka_topic_user_actions}_dlq", num_partitions=1, replication_factor=1),
    NewTopic(name=f"{settings.kafka_topic_orders}_dlq",       num_partitions=1, replication_factor=1),
    NewTopic(name=f"{settings.kafka_topic_transactions}_dlq",  num_partitions=1, replication_factor=1),
]


def create_topics() -> None:
    admin = KafkaAdminClient(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        client_id="topic-admin",
    )

    topics_to_create = []
    for topic in TOPIC_DEFINITIONS:
        try:
            topics_to_create.append(topic)
        except Exception:
            pass

    if not topics_to_create:
        logger.info("✅ No new topics to create.")
        return

    try:
        admin.create_topics(new_topics=topics_to_create, validate_only=False)
        for topic in topics_to_create:
            logger.info("✅ Created topic: %-30s | partitions=%d", topic.name, topic.num_partitions)
    except TopicAlreadyExistsError:
        logger.info("ℹ️  Topics already exist — skipping creation")
    except Exception as e:
        logger.error("❌ Failed to create topics: %s", e)
        sys.exit(1)
    finally:
        admin.close()


if __name__ == "__main__":
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logger.info("  📋  Kafka Topic Initializer")
    logger.info("  Broker: %s", settings.kafka_bootstrap_servers)
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    create_topics()
