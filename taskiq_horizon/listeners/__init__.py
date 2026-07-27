"""
Horizon Event Listeners — single-responsibility handlers for domain events.

Each listener ports one of the upstream listeners.
All listeners have a `handle(event)` method, which may be sync or async.
"""
