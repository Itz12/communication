from __future__ import annotations

import json
import threading
import time
import uuid
from typing import Any, Dict, Optional

from .producer import AccountsProducer

_accounts_producer: Optional[AccountsProducer] = None
_accounts_lock = threading.RLock()


def get_publisher(start: bool = False) -> AccountsProducer:
    """Return the Kafka-backed producer used by notification compatibility APIs."""
    if start:
        raise RuntimeError("The Kafka producer is started lazily when an event is published")
    return get_accounts_producer()


def get_accounts_producer() -> AccountsProducer:
    global _accounts_producer
    with _accounts_lock:
        if _accounts_producer is None:
            _accounts_producer = AccountsProducer()
        return _accounts_producer


def _pack_envelope(subject: str, payload: Dict[str, Any], event_id: Optional[str] = None) -> Dict[str, Any]:
    return {
        "event_id": event_id or str(uuid.uuid4()),
        "subject": subject,
        "timestamp": time.time(),
        "payload": payload,
    }


def _to_bytes(obj: Dict[str, Any]) -> bytes:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")


async def publish_notification_event_async(
    subject: str,
    payload: Dict[str, Any],
    *,
    event_id: Optional[str] = None,
    use_jetstream: bool = False,
    trace_id: Optional[str] = None,
    publisher: Optional[AccountsProducer] = None,
) -> None:
    del use_jetstream
    producer = publisher or get_accounts_producer()
    await producer.publish_event_async(
        payload,
        topic=subject,
        event_type=subject,
        event_id=event_id,
        trace_id=trace_id,
    )


def publish_notification_event_sync(
    subject: str,
    payload: Dict[str, Any],
    *,
    event_id: Optional[str] = None,
    use_jetstream: bool = False,
    trace_id: Optional[str] = None,
    publisher: Optional[AccountsProducer] = None,
) -> None:
    del use_jetstream
    (publisher or get_accounts_producer()).publish_event(
        payload,
        topic=subject,
        event_type=subject,
        event_id=event_id,
        trace_id=trace_id,
    )
