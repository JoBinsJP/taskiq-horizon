"""
Alatazone: A port of Laravel Horizon for TaskIQ.

Bring a beautiful monitoring dashboard and queue metrics to TaskIQ.
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("taskiq-horizon")
except PackageNotFoundError:  # pragma: no cover — package not installed (e.g. source checkout)
    __version__ = "0.0.0"

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
