import ast
import base64
import json
import pickle
import time
from dataclasses import dataclass, field
from typing import Any

from taskiq.message import TaskiqMessage


@dataclass
class TaskPayload:
    """
    Wrapper around TaskiqMessage — plays the role of the upstream job payload.
    Adds stateful properties like tags, silenced flag, and push timings.
    """

    # Original task message
    message: TaskiqMessage

    # The queue connection name (e.g. 'redis')
    connection: str = "redis"

    # Decoded values, similar to Horizon's $this->decoded
    # It stores standard taskiq info + horizon specific overrides
    decoded: dict[str, Any] = field(default_factory=dict)

    # The raw string payload for Redis
    value: str = ""

    def __post_init__(self):
        pushed_at = self.message.labels.get("horizon_pushed_at", time.time())

        self.decoded = {
            "uuid": self.message.task_id,
            "id": self.message.task_id,
            "task_name": self.message.task_name,
            "displayName": self.message.task_name,
            "job": self.message.task_name,
            "attempts": self._determine_attempts(),
            "maxTries": self.message.labels.get("max_retries", 0),
            "maxExceptions": 0,
            "backoff": 0,
            "timeout": self.message.labels.get("timeout", 60),
            "retryUntil": None,
            "data": {
                "commandName": self.message.task_name,
            },
            "taskiq": {
                "state": self._encode_message_state(),
            },
        }

        retry_of = self.message.labels.get("retry_of")
        if retry_of:
            self.decoded["retry_of"] = retry_of

        self.decoded["pushedAt"] = str(pushed_at)

    def prepare(self, config: Any) -> "TaskPayload":
        """
        Prepare the payload for storage by adding tags, silenced checks, etc.
        """
        tags = self._determine_tags()
        silenced = self._should_be_silenced(config, tags)

        self.set(
            {
                "type": "job",
                "tags": tags,
                "silenced": silenced,
                "pushedAt": self.decoded.get("pushedAt", str(time.time())),
            }
        )

        return self

    def _determine_attempts(self) -> int:
        """
        Derive the attempt number for the dashboard's "Attempts" field.

        taskiq's retry middlewares (SimpleRetryMiddleware / SmartRetryMiddleware)
        stamp the number of prior retries on the "_retries" label, so the
        current attempt is that count plus one. First runs carry no label.
        """
        try:
            return int(self.message.labels.get("_retries", 0)) + 1
        except (TypeError, ValueError):
            return 1

    def _encode_message_state(self) -> str:
        state = {
            "labels": dict(self.message.labels),
            "labels_types": self.message.labels_types,
            "args": list(self.message.args),
            "kwargs": dict(self.message.kwargs),
        }
        return base64.b64encode(pickle.dumps(state)).decode("ascii")

    def _determine_tags(self) -> list[str]:
        """
        Extract user-defined tags from the task labels.

        The canonical format is a comma-separated string, since taskiq
        stringifies label values on the wire:

            @broker.task(tags="tenant:42,billing")

        Real lists are also accepted (``tags=["a", "b"]``), and stringified
        lists produced by taskiq's label coercion (``"['a', 'b']"``) are
        defensively parsed back into their elements.
        """
        tags = self.message.labels.get("tags", [])

        if isinstance(tags, str):
            stripped = tags.strip()
            # Defensively parse a stringified list ("['a', 'b']") — taskiq
            # coerces list labels to their repr when serializing messages.
            if stripped.startswith("[") and stripped.endswith("]"):
                try:
                    literal = ast.literal_eval(stripped)
                    if isinstance(literal, (list, tuple, set)):
                        return [str(tag) for tag in literal if str(tag).strip()]
                except (ValueError, SyntaxError):
                    pass
            # Canonical comma-separated format: "a,b" → ["a", "b"]
            return [part.strip() for part in tags.split(",") if part.strip()]

        if isinstance(tags, (list, tuple, set)):
            return [str(tag) for tag in tags]

        return [str(tags)] if tags else []

    def _should_be_silenced(self, config: Any, tags: list[str]) -> bool:
        if self.message.task_name in config.silenced:
            return True

        return any(tag in config.silenced_tags for tag in tags)

    def set(self, values: dict[str, Any]) -> "TaskPayload":
        self.decoded.update(values)
        self.value = json.dumps(self.decoded)
        return self

    def id(self) -> str:
        return self.message.task_id

    def tags(self) -> list[str]:
        return self.decoded.get("tags", [])

    def is_retry(self) -> bool:
        return "retry_of" in self.decoded

    def retry_of(self) -> str | None:
        return self.decoded.get("retry_of")

    def is_silenced(self) -> bool:
        return self.decoded.get("silenced", False)

    def command_name(self) -> str:
        return self.message.task_name

    def display_name(self) -> str:
        return self.message.task_name
