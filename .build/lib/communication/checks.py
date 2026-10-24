from __future__ import annotations


def communication_checks(app_configs=None, **kwargs):
    from django.conf import settings
    from django.core.checks import Error, Warning

    from .config import CommunicationSettings

    messages = []
    try:
        config = CommunicationSettings.from_django_settings(settings)
    except Exception as exc:
        return [Error("Communication settings are invalid.", hint=type(exc).__name__, id="communication.E001")]

    installed = set(getattr(settings, "INSTALLED_APPS", ()))
    if "communication" not in installed and "communication.apps.CommunicationConfig" not in installed:
        messages.append(Warning("The communication app is not installed.", id="communication.W001"))
    configured_transports = set(config.enabled_transports) | set(config.consumer_transports)
    if "kafka" in configured_transports:
        if config.kafka.security_protocol != "SASL_SSL":
            messages.append(
                Error(
                    "Kafka clients must use SASL_SSL with the platform.",
                    id="communication.E006",
                )
            )
        if config.kafka.acks not in {"all", "-1"}:
            messages.append(
                Error(
                    "Kafka producers must use acks=all with the platform.",
                    id="communication.E007",
                )
            )
        try:
            import confluent_kafka  # noqa: F401
        except ImportError:
            messages.append(Error("Kafka is enabled but confluent-kafka is not installed.", id="communication.E004"))
    return messages
