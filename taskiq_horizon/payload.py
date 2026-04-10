import json
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from taskiq.message import TaskiqMessage

@dataclass
class TaskPayload:
    """
    Wrapper around TaskiqMessage analogous to Laravel Horizon's JobPayload.
    Adds stateful properties like tags, silenced flag, and push timings.
    """
    
    # Original task message
    message: TaskiqMessage
    
    # The queue connection name (e.g. 'redis')
    connection: str = "redis"
    
    # Decoded values, similar to Horizon's $this->decoded
    # It stores standard taskiq info + horizon specific overrides
    decoded: Dict[str, Any] = field(default_factory=dict)
    
    # The raw string payload for Redis
    value: str = ""
    
    def __post_init__(self):
        self.decoded = {
            "uuid": self.message.task_id,
            "id": self.message.task_id,
            "task_name": self.message.task_name,
            "displayName": self.message.task_name,
            "job": self.message.task_name,
            "maxTries": self.message.labels.get("max_retries", 0),
            "maxExceptions": 0,
            "backoff": 0,
            "timeout": self.message.labels.get("timeout", 60),
            "retryUntil": None,
            "data": {
                "commandName": self.message.task_name,
            },
        }
        
    def prepare(self, config: Any) -> "TaskPayload":
        """
        Prepare the payload for storage by adding tags, silenced checks, etc.
        """
        tags = self._determine_tags()
        silenced = self._should_be_silenced(config, tags)
        
        self.set({
            "type": "job",
            "tags": tags,
            "silenced": silenced,
            "pushedAt": str(time.time()),
        })
        
        return self
        
    def _determine_tags(self) -> List[str]:
        # User defined tags in task labels
        tags = self.message.labels.get("tags", [])
        if not isinstance(tags, list):
            tags = [tags]
            
        # By default, append the task name as a tag, maybe?
        return tags
        
    def _should_be_silenced(self, config: Any, tags: List[str]) -> bool:
        if self.message.task_name in config.silenced:
            return True
            
        return any(tag in config.silenced_tags for tag in tags)
        
    def set(self, values: Dict[str, Any]) -> "TaskPayload":
        self.decoded.update(values)
        self.value = json.dumps(self.decoded)
        return self
        
    def id(self) -> str:
        return self.message.task_id
        
    def tags(self) -> List[str]:
        return self.decoded.get("tags", [])
        
    def is_retry(self) -> bool:
        return "retry_of" in self.decoded
        
    def retry_of(self) -> Optional[str]:
        return self.decoded.get("retry_of")
        
    def is_silenced(self) -> bool:
        return self.decoded.get("silenced", False)
        
    def command_name(self) -> str:
        return self.message.task_name
        
    def display_name(self) -> str:
        return self.message.task_name
