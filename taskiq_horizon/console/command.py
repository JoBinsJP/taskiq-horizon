"""Commands use the Alata kernel that registered them."""

import asyncio

from alata.foundation.console import Command


class HorizonCommandBase(Command):
    def alata_application(self):
        app = getattr(self, "container", None)
        if app is None:
            from alata.application import boot
            from alata.container import container

            boot()
            app = container
        return app

    def run_async(self, coroutine):
        async def run():
            try:
                return await coroutine
            finally:
                # Commands may run repeatedly through the scheduler. Release
                # this loop's Redis sockets before asyncio.run closes the loop.
                await self.alata_application().make("horizon.redis").aclose()

        return asyncio.run(run())
