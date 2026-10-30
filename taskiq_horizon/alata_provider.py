"""Alata's native Horizon provider."""

import importlib

from alata.support.service_provider import ServiceProvider
from redis.asyncio import Redis

from taskiq_horizon.service_provider import HorizonServiceProvider


class HorizonProvider(ServiceProvider):
    def __init__(self, app, config=None):
        super().__init__(app)
        self.config = config
        self.package = HorizonServiceProvider(app)
        self._redis = None

    def register(self):
        config = self.config
        if config is None:
            module = importlib.import_module(f"{self.app.config_module}.horizon")
            config = module.horizon_config
        self.app.instance("horizon.config", config)
        self.app.singleton("horizon.redis", self._create_redis)
        self.package.register()
        self.app.add_commands(self.package.commands())

    def _create_redis(self):
        self._redis = Redis.from_url(
            self.app.make("horizon.config").redis_url,
            decode_responses=False,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
        return self._redis

    def boot(self):
        broker = self.app.make("queue.broker")
        middleware = self.app.make("horizon.middleware")
        if middleware not in broker.middlewares:
            broker.add_middlewares(middleware)

    def boot_http(self, fastapi):
        from alata.queue.batch_repository import BatchRepository

        self.package.mount_dashboard(fastapi, broker=self.app.make("queue.broker"), batch_repo=BatchRepository)

    async def shutdown(self):
        if self._redis is not None:
            await self._redis.aclose()
