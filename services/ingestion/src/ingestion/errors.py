class InvalidEvent(Exception):
    """A record that cannot be applied and should be quarantined."""

    def __init__(self, error_type: str, detail: str) -> None:
        self.error_type = error_type
        self.detail = detail
        super().__init__(f"{error_type}: {detail}")


class TransientStorageError(Exception):
    """The database could not durably accept the record. Do not commit the offset."""


class CommitFailed(Exception):
    """The database commit succeeded or was unnecessary, but the offset commit failed."""


class PartitionLost(Exception):
    """This process no longer owns the partition. Do not commit the offset."""
