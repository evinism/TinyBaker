from typing import Set


class BakerError(Exception):
    pass


class TagConflictError(Exception):
    pass


class FileSetError(BakerError):
    def __init__(self, actual: Set[str], expected: Set[str]):
        messages = []
        if len(actual - expected) > 0:
            messages.append(
                "Extra files: {}".format(", ".join(list(actual - expected)))
            )
        if len(expected - actual) > 0:
            messages.append(
                "Missing files: {}".format(", ".join(list(expected - actual)))
            )
        super().__init__("; ".join(messages))


class CircularFileSetError(BakerError):
    pass
