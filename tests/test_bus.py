from __future__ import annotations

import asyncio
from typing import Mapping, Sequence

import pytest

from communication.bus import EventBus
from communication.config import CommunicationSettings
from communication.errors import PublishError
from communication.protocols import MessageCallback, TransportPublishResult
from communication.registry import TransportRegistry


class FakeSubscription:
    async def close(self) -> None:
        return None


class FakeTransport:
    durable = True

    def __init__(self, name: str, *, fail: bool = False) -> None:
        self.name = name
        self.fail = fail
        self.started = False
        self.events = []

    async def start(self) -> None:
        self.started = True

    async def close(self) -> None:
        self.started = False

    async def flush(self, timeout: float = 10.0) -> None:
        return None

    async def publish(self, *, topic, payload, headers, key, timeout):
        if self.fail:
            raise RuntimeError("unavailable")
        self.events.append((topic, payload, dict(headers), key))
        return TransportPublishResult(self.name, topic, headers["event-id"], True)

    async def subscribe(self, *, topics: Sequence[str], group_id: str, callback: MessageCallback, concurrency: int, max_pending: int):
        return FakeSubscription()

    async def health(self) -> Mapping[str, object]:
        return {"healthy": not self.fail}


def config(mode: str) -> CommunicationSettings:
    return CommunicationSettings.from_mapping(
        {
            "SERVICE_NAME": "authentication",
            "ENABLED_TRANSPORTS": ("kafka",),
            "PRIMARY_TRANSPORT": "kafka",
            "PUBLISH_MODE": mode,
            "CONSUMER_TRANSPORTS": ("kafka",),
            "KAFKA": {"BOOTSTRAP_SERVERS": "kafka:9092"},
        }
    )


@pytest.mark.asyncio
async def test_kafka_mode_delivers_event_through_canonical_transport() -> None:
    settings = config("primary")
    registry = TransportRegistry(settings)
    kafka = FakeTransport("kafka")
    registry.register(kafka)
    bus = EventBus(settings, registry=registry)
    event = bus.build_event(event_type="accounts.user.created", data={"user_id": "1"}, topic="accounts.user.created")
    result = await bus.publish(event)
    assert result.successful
    assert result.succeeded_transports == ("kafka",)
    assert kafka.events[0][2]["event-id"] == event.event_id


def test_build_event_accepts_an_explicit_schema_version() -> None:
    settings = config("primary")
    bus = EventBus(settings, registry=TransportRegistry(settings))

    event = bus.build_event(
        event_type="assignments.created",
        data={"assignment_id": "1"},
        topic="assignments.created",
        schema_version="v2",
    )

    assert event.schema_version == "v2"
    assert event.transport_headers()["event-version"] == "v2"


@pytest.mark.asyncio
async def test_kafka_failure_is_reported_without_fallback_transport() -> None:
    settings = config("primary")
    registry = TransportRegistry(settings)
    registry.register(FakeTransport("kafka", fail=True))
    bus = EventBus(settings, registry=registry)
    with pytest.raises(PublishError):
        await bus.emit(event_type="accounts.user.created", data={}, topic="accounts.user.created")


def test_health_report_contains_no_configuration_secrets() -> None:
    async def scenario() -> None:
        settings = config("primary")
        registry = TransportRegistry(settings)
        registry.register(FakeTransport("kafka"))
        bus = EventBus(settings, registry=registry)
        await bus._ensure_started(("kafka",))

        health = await bus.health()

        assert health == {
            "kafka": {"healthy": True},
        }
        assert "password" not in repr(health).casefold()
        await bus.close()

    asyncio.run(scenario())
