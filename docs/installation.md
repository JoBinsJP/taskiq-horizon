# Installation

TaskIQ Horizon currently relies on Redis for storing its real-time metrics and job data. 

To install the package into your project, use pip:

```bash
pip install taskiq-horizon
```

*(Note: If you are using this locally in editable mode, you can install it via `uv pip install -e path/to/taskiq-horizon`)*

## Basic Setup

Because TaskIQ Horizon integrates directly with FastAPI (or any ASGI app) to serve its dashboard, the setup involves two parts: **1) The Host App Configuration** and **2) The Supervisor Process**.

### 1. The Configuration File

Create a `horizon.py` configuration file in your host application to define how Horizon should connect to Redis and how it should behave.

```python
# config/horizon.py
from taskiq_horizon.config import HorizonConfig, TrimSettings
from myapp.settings import settings

horizon_config = HorizonConfig(
    prefix="myapp-horizon:",
    redis_url=settings.REDIS_URL,
    trim=TrimSettings(
        recent=60,         # Keep recent jobs for 1 hour
        pending=60,        # Keep pending jobs for 1 hour
        completed=60,      # Keep completed jobs for 1 hour
        failed=10080,      # Keep failed jobs for 7 days
    ),
    waits={"default": 60, "high": 30},
)
```

### 2. Mounting the Dashboard

In your FastAPI lifespan or application factory, you need to register the Horizon services into a Dependency Injection container, attach the middleware to your broker, and mount the routes:

```python
from fastapi import FastAPI
from taskiq_horizon.service_provider import register_services, mount_dashboard
from config.horizon import horizon_config
from myapp.broker import broker
from my_di_container import container  # Your DI container

app = FastAPI()

# 1. Bind the config and Redis connection to the container
container.singleton("horizon.config", lambda: horizon_config)
container.singleton("horizon.redis", lambda: aioredis.from_url(horizon_config.redis_url))

# 2. Register Horizon services (repositories, wait time calculators, etc.)
register_services(container)

# 3. Attach Horizon's tracking middleware to your TaskIQ broker
middleware = container.make("horizon.middleware")
broker.add_middlewares(middleware)

# 4. Mount the dashboard routes
mount_dashboard(app, container)
```

### 3. Running the Supervisor

Instead of starting standard TaskIQ workers natively, you boot the **TaskIQ Horizon Master Supervisor**. This daemon orchestrates all child supervisors and child worker processes dynamically.

You can create a standalone entry point for it:

```python
# run_horizon.py
import asyncio
import logging
from taskiq_horizon.master_supervisor import MasterSupervisor
from my_di_container import container

async def run():
    logging.basicConfig(level=logging.INFO)
    
    # Resolves all repositories registered earlier
    master = MasterSupervisor(
        name="master-1",
        container=container,
    )
    await master.run()

if __name__ == "__main__":
    asyncio.run(run())
```

Run this script to start processing jobs and watch them appear in the dashboard!
