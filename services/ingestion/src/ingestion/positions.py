from dataclasses import dataclass


@dataclass(frozen=True)
class KafkaPosition:
    topic: str
    partition: int
    offset: int
    key: str | None
