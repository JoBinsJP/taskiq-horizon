"""
Alatazone: A port of Laravel Horizon for TaskIQ.

Bring a beautiful monitoring dashboard and queue metrics to TaskIQ.
"""

__version__ = "0.1.0"

from .config import HorizonConfig
from .event_dispatcher import EventDispatcher
from .middleware import HorizonMiddleware
from .supervisor import Supervisor, SupervisorOptions
from .master_supervisor import MasterSupervisor
from .service_provider import register_services, mount_dashboard

__all__ = [
    "HorizonConfig",
    "EventDispatcher",
    "HorizonMiddleware",
    "MasterSupervisor",
    "Supervisor",
    "SupervisorOptions",
    "register_services",
    "mount_dashboard",
]

