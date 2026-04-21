import asyncio
import json
import logging
import uuid
from typing import Any, Dict, List, Optional, Union

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from core.config import settings
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class KafkaClient:
    """A unified Kafka client wrapping AIOKafka for both production and consumption."""

    def __init__(self, bootstrap_servers: str = settings.KAFKA_BOOTSTRAP_SERVERS):
        self.bootstrap_servers = bootstrap_servers
        self._producer: Optional[AIOKafkaProducer] = None
        self._consumers: Dict[str, AIOKafkaConsumer] = {}

    async def start_producer(self):
        """Initializes the underlying producer."""
        if self._producer:
            return

        try:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: (
                    str(k).encode("utf-8") if k is not None else None
                ),
                acks="all",
                request_timeout_ms=10000,
                retry_backoff_ms=500,
                max_batch_size=16384,
                linger_ms=10,
            )
            await self._producer.start()
            logger.info(f"Kafka producer started on {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to start producer: {e}")
            raise

    async def stop_producer(self):
        if self._producer:
            await self._producer.stop()
            self._producer = None
            logger.info("Kafka producer stopped")

    async def create_consumer(
        self, topics: List[str], group_id: str
    ) -> AIOKafkaConsumer:
        """Creates and starts a consumer for specific topics."""
        consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
        )
        await consumer.start()
        logger.info(f"Kafka consumer started for topics {topics} in group {group_id}")
        return consumer

    async def send(self, topic: str, value: Any, key: Any = None) -> Any:
        if not self._producer:
            raise RuntimeError("Producer not started")
        return await self._producer.send_and_wait(topic, value=value, key=key)

    async def send_batch(self, topic: str, messages: List[Dict]) -> int:
        """Sends a list of messages (value and optional key) concurrently."""
        if not self._producer:
            raise RuntimeError("Producer not started")

        tasks = []
        for msg in messages:
            tasks.append(
                self._producer.send(topic, value=msg["value"], key=msg.get("key"))
            )

        results = await asyncio.gather(*tasks, return_exceptions=True)
        success_count = sum(1 for r in results if not isinstance(r, Exception))
        return success_count


class KafkaProducerService:
    """Wrapper service for specific data faker production tasks."""

    def __init__(self, client: KafkaClient):
        self.client = client

    async def start(self):
        await self.client.start_producer()

    async def stop(self):
        await self.client.stop_producer()

    def _serialize_value(self, value: Union[Dict, BaseModel]) -> Dict:
        if isinstance(value, BaseModel):
            return value.model_dump(mode="json")
        return value

    async def produce_message(
        self,
        topic: str,
        value: Union[Dict, BaseModel],
        key: Optional[Union[str, uuid.UUID]] = None,
    ) -> bool:
        try:
            serialized_value = self._serialize_value(value)
            await self.client.send(topic, value=serialized_value, key=key)
            return True
        except Exception as e:
            logger.error(f"Failed to produce message to {topic}: {e}")
            return False

    async def produce_batch(self, topic: str, messages: List[Dict[str, Any]]) -> int:
        try:
            formatted_messages = []
            for m in messages:
                formatted_messages.append(
                    {
                        "value": self._serialize_value(m.get("value")),
                        "key": m.get("key"),
                    }
                )
            return await self.client.send_batch(topic, formatted_messages)
        except Exception as e:
            logger.error(f"Batch production failed: {e}")
            return 0


kafka_client = KafkaClient()
kafka_producer_service = KafkaProducerService(kafka_client)
