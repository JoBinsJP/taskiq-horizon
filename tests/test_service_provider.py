from unittest.mock import MagicMock, patch

from taskiq_horizon.service_provider import HorizonServiceProvider


def test_horizon_service_provider_mount_dashboard_forwards_broker_and_batch_repo():
    container = MagicMock()
    fastapi_app = MagicMock()
    broker = object()
    batch_repo = object()

    provider = HorizonServiceProvider(container)

    with patch("taskiq_horizon.service_provider.mount_dashboard") as mount_dashboard:
        provider.mount_dashboard(fastapi_app, broker=broker, batch_repo=batch_repo)

    mount_dashboard.assert_called_once_with(
        fastapi_app,
        container,
        broker=broker,
        batch_repo=batch_repo,
    )
