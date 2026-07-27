import time


class Stopwatch:
    """
    Port of the upstream stopwatch for managing graceful zero-downtime timeouts.
    """

    def __init__(self):
        self.starts = {}

    def start(self, key: str) -> None:
        """Record the current time for the given key."""
        self.starts[key] = time.time()

    def check(self, key: str) -> float:
        """Return elapsed time in seconds since start was called for the key."""
        if key not in self.starts:
            return 0.0
        return time.time() - self.starts[key]
