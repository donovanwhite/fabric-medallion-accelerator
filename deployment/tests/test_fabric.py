"""Focused Fabric connection preflight tests."""

from __future__ import annotations

import json
import unittest
import urllib.error
from typing import Any
from unittest.mock import patch

from fabric_medallion_deployer.fabric import (
    ApiResponse,
    FabricApiError,
    FabricClient,
)

CONNECTION_ID = "11111111-1111-1111-1111-111111111111"


def response(payload: dict[str, Any]) -> ApiResponse:
    """Build one synchronous JSON response."""
    return ApiResponse(
        status_code=200,
        headers={},
        content=json.dumps(payload).encode("utf-8"),
    )


class FakeFabricClient(FabricClient):
    """Isolated Fabric client with deterministic connection responses."""

    def __init__(self, connection_status: dict[str, Any]) -> None:
        super().__init__(lambda: "token")
        self.connection_status = connection_status
        self.connection = {
            "id": CONNECTION_ID,
            "displayName": "conn_bas_finance_sql",
            "connectivityType": "ShareableCloud",
            "connectionDetails": {"type": "FabricSql"},
            "credentialDetails": {"credentialType": "OAuth2"},
        }
        self.connections = [self.connection]

    def paged(self, path: str) -> list[dict[str, Any]]:
        if path != "/connections":
            raise AssertionError(f"Unexpected collection request: {path}")
        return self.connections

    def request(
        self,
        method: str,
        path_or_url: str,
        *,
        body: dict[str, Any] | None = None,
        expected: tuple[int, ...] = (200,),
    ) -> ApiResponse:
        del body, expected
        if method == "GET" and path_or_url == f"/connections/{CONNECTION_ID}":
            return response(self.connection)
        if (
            method == "POST"
            and path_or_url == f"/connections/{CONNECTION_ID}/testConnection"
        ):
            return response(self.connection_status)
        raise AssertionError(f"Unexpected request: {method} {path_or_url}")


class FabricConnectionTests(unittest.TestCase):
    """Verify connection resolution and health gates."""

    def test_resolves_online_oauth_connection_by_name(self) -> None:
        client = FakeFabricClient({"status": "Online", "errors": []})

        connection = client.resolve_oauth2_connection(
            "conn_bas_finance_sql", "FabricSql"
        )
        client.validate_connection_online(connection)

        self.assertEqual(connection["id"], CONNECTION_ID)

    def test_resolves_online_oauth_connection_by_id(self) -> None:
        client = FakeFabricClient({"status": "Online", "errors": []})

        connection = client.resolve_oauth2_connection(CONNECTION_ID, "FabricSql")

        self.assertEqual(connection["displayName"], "conn_bas_finance_sql")

    def test_rejects_expired_oauth_connection_before_deployment(self) -> None:
        client = FakeFabricClient(
            {
                "status": "Offline",
                "errors": [
                    {
                        "errorCode": "IncorrectCredentials",
                        "message": "OAuth refresh token expired",
                    }
                ],
            }
        )

        with self.assertRaisesRegex(FabricApiError, "OAuth refresh token expired"):
            client.validate_connection_online(client.connection)

    def test_missing_connection_has_actionable_prerequisite_error(self) -> None:
        client = FakeFabricClient({"status": "Online", "errors": []})
        client.connections = []

        with self.assertRaisesRegex(
            FabricApiError, "Create and authorize the connection"
        ):
            client.resolve_oauth2_connection("conn_bas_finance_sql", "FabricSql")

    def test_duplicate_connection_name_requires_explicit_id(self) -> None:
        client = FakeFabricClient({"status": "Online", "errors": []})
        client.connections.append(
            {
                **client.connection,
                "id": "22222222-2222-2222-2222-222222222222",
            }
        )

        with self.assertRaisesRegex(FabricApiError, "pass its ID"):
            client.resolve_oauth2_connection("conn_bas_finance_sql", "FabricSql")

    def test_mutating_request_is_not_retried_after_server_error(self) -> None:
        client = FabricClient(lambda: "token")
        error = urllib.error.HTTPError(
            "https://api.fabric.microsoft.com/v1/workspaces/workspace/items",
            503,
            "Unavailable",
            {},
            None,
        )
        with (
            patch("urllib.request.urlopen", side_effect=error) as send,
            patch("fabric_medallion_deployer.fabric.time.sleep") as sleep,
            self.assertRaises(FabricApiError),
        ):
            client.request(
                "POST", "/workspaces/workspace/items", body={"displayName": "demo"}
            )
        self.assertEqual(send.call_count, 1)
        sleep.assert_not_called()

    def test_regional_operation_location_is_polled_through_public_api(self) -> None:
        client = FabricClient(lambda: "token")
        with (
            patch.object(
                client, "request", return_value=response({"status": "Succeeded"})
            ) as send,
            patch("fabric_medallion_deployer.fabric.time.sleep"),
        ):
            client._wait_for_operation(
                "https://wabi-west-us3-a-primary-redirect.analysis.windows.net/v1/operations/operation-test",
                "test deployment",
            )
        self.assertEqual(
            send.call_args.args,
            ("GET", "https://api.fabric.microsoft.com/v1/operations/operation-test"),
        )

    def test_accepted_operation_is_recorded_before_polling(self):
        events = []
        location = "https://api.fabric.microsoft.com/v1/operations/operation-test"
        client = FabricClient(
            lambda: "token", operation_observer=lambda receipt: events.append(receipt)
        )

        def wait(value, name):
            self.assertEqual(
                events,
                [
                    {
                        "method": "POST",
                        "request_path": "/workspaces/test/items",
                        "operation_location": location,
                    }
                ],
            )
            return {"status": "Succeeded"}

        with (
            patch.object(
                client,
                "request",
                return_value=ApiResponse(202, {"Location": location}, b""),
            ),
            patch.object(client, "_wait_for_operation", side_effect=wait),
        ):
            self.assertEqual(
                client.request_lro("POST", "/workspaces/test/items")["status"],
                "Succeeded",
            )


if __name__ == "__main__":
    unittest.main()
