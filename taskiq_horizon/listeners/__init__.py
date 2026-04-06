"""
Horizon Event Listeners — single-responsibility handlers for domain events.

Each listener mirrors a class from Laravel Horizon's Listeners/ directory.
All listeners have a `handle(event)` method, which may be sync or async.
"""
