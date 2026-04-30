"""
Alatazone: A port of Laravel Horizon for TaskIQ.

Bring a beautiful monitoring dashboard and queue metrics to TaskIQ.
"""

__version__ = "0.1.0"

from .config import HorizonConfig
from .event_dispatcher import EventDispatcher
from .master_supervisor import MasterSupervisor
from .middleware import HorizonMiddleware
from .service_provider import mount_dashboard, register_services
from .supervisor import Supervisor, SupervisorOptions

__all__ = [
    "EventDispatcher",
    "HorizonConfig",
    "HorizonMiddleware",
    "MasterSupervisor",
    "Supervisor",
    "SupervisorOptions",
    "mount_dashboard",
    "register_services",
]
