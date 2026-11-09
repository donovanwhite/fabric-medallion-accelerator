# Copyright (c) Microsoft Corporation.
# SPDX-License-Identifier: MIT
"""Offline contract tests for the documented MLV REST routes."""

from __future__ import annotations

import json
import unittest
import urllib.error
from unittest.mock import patch

from fabric_medallion_deployer.fabric import ApiResponse, FabricApiError, FabricClient

WS = "11111111-1111-1111-1111-111111111111"
LH = "22222222-2222-2222-2222-222222222222"
JOB = "33333333-3333-3333-3333-333333333333"
EXEC = "44444444-4444-4444-4444-444444444444"
BASE = f"/workspaces/{WS}/lakehouses/{LH}"
LOCATION = f"https://api.fabric.microsoft.com/v1/workspaces/{WS}/items/{LH}/jobs/instances/{JOB}"


class MlvClientTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = FabricClient(lambda: "offline-test-token")

    def test_submit_once_uses_lakehouse_route_and_location_not_lro(self) -> None:
        with (
            patch.object(
                self.client,
                "request",
                return_value=ApiResponse(
                    202, {"Location": LOCATION, "Retry-After": "60"}, b""
                ),
            ) as send,
            patch.object(self.client, "_wait_for_operation") as wait,
        ):
            result = self.client.submit_mlv_refresh(
                WS, LH, execution_definition_id=EXEC
            )
        self.assertEqual(
            result, {"job_id": JOB, "location": LOCATION, "retry_after": 60}
        )
        send.assert_called_once_with(
            "POST",
            f"{BASE}/jobs/refreshMaterializedLakeViews/instances",
            body={"executionData": {"mlvExecutionDefinitionId": EXEC}},
            expected=(202,),
        )
        wait.assert_not_called()

    def test_full_lineage_submit_has_no_body(self) -> None:
        location = LOCATION.replace("/items/", "/lakehouses/")
        with patch.object(
            self.client,
            "request",
            return_value=ApiResponse(
                202, {"Location": location, "Retry-After": "0"}, b""
            ),
        ) as send:
            self.assertEqual(self.client.submit_mlv_refresh(WS, LH)["job_id"], JOB)
        self.assertIsNone(send.call_args.kwargs["body"])

    def test_untrusted_or_incomplete_receipts_fail_without_polling(self) -> None:
        for headers in (
            {},
            {"Location": LOCATION},
            {"Location": LOCATION, "Retry-After": "-1"},
            {"Location": LOCATION, "Retry-After": "n/a"},
            {"Location": LOCATION.replace(JOB, "not-a-job"), "Retry-After": "1"},
            {
                "Location": LOCATION.replace("api.fabric.microsoft.com", "example.com"),
                "Retry-After": "1",
            },
            {"Location": LOCATION.replace(LH, EXEC), "Retry-After": "1"},
            {"Location": LOCATION + "?token=bad", "Retry-After": "1"},
        ):
            with (
                self.subTest(headers=headers),
                patch.object(
                    self.client, "request", return_value=ApiResponse(202, headers, b"")
                ) as send,
                self.assertRaises(FabricApiError),
            ):
                self.client.submit_mlv_refresh(WS, LH)
            send.assert_called_once()

    def test_schedules_and_history_use_documented_collections(self) -> None:
        with patch.object(self.client, "_mlv_collection", return_value=[]) as paged:
            self.client.list_mlv_refresh_schedules(WS, LH)
            self.client.list_mlv_refresh_jobs(WS, LH)
        self.assertEqual(
            [call.args[0] for call in paged.call_args_list],
            [
                f"{BASE}/jobs/refreshMaterializedLakeViews/schedules",
                f"{BASE}/jobs/refreshMaterializedLakeViews/instances",
            ],
        )

    def test_collection_follows_token_only_pages_and_rejects_bad_continuations(self):
        def response(payload):
            return ApiResponse(200, {}, json.dumps(payload).encode())

        with patch.object(
            self.client,
            "request",
            side_effect=[
                response({"value": [{"id": "one"}], "continuationToken": "a+b"}),
                response({"value": [{"id": "two"}]}),
            ],
        ) as send:
            self.assertEqual(
                self.client.list_mlv_refresh_jobs(WS, LH),
                [{"id": "one"}, {"id": "two"}],
            )
        self.assertTrue(send.call_args.args[1].endswith("continuationToken=a%2Bb"))
        with (
            patch.object(
                self.client,
                "request",
                return_value=response(
                    {
                        "value": [],
                        "continuationUri": "https://example.com/v1/jobs",
                    }
                ),
            ),
            self.assertRaises(FabricApiError),
        ):
            self.client.list_mlv_refresh_jobs(WS, LH)

    def test_execution_definition_is_not_fictional_lineage_endpoint(self) -> None:
        with patch.object(
            self.client,
            "request",
            return_value=ApiResponse(200, {}, json.dumps({"id": EXEC}).encode()),
        ) as send:
            self.assertEqual(
                self.client.get_mlv_execution_definition(WS, LH, EXEC)["id"], EXEC
            )
        send.assert_called_once_with("GET", f"{BASE}/mlvexecutiondefinitions/{EXEC}")

    def test_get_and_cancel_are_item_scoped_and_do_not_poll(self) -> None:
        with patch.object(
            self.client,
            "request",
            return_value=ApiResponse(200, {}, b'{"status":"Completed"}'),
        ) as send:
            self.client.get_mlv_refresh_job(WS, LH, JOB)
            self.client.cancel_mlv_refresh_job(WS, LH, JOB)
        path = f"/workspaces/{WS}/items/{LH}/jobs/instances/{JOB}"
        self.assertEqual(send.call_args_list[0].args, ("GET", path))
        self.assertEqual(send.call_args_list[1].args, ("POST", path + "/cancel"))
        self.assertEqual(send.call_args_list[1].kwargs["expected"], (202,))

    def test_invalid_identifiers_do_not_reach_transport(self) -> None:
        with (
            patch.object(self.client, "request") as send,
            self.assertRaises(ValueError),
        ):
            self.client.submit_mlv_refresh("../workspace", LH)
        send.assert_not_called()

    def test_submit_transport_failure_is_not_retried(self) -> None:
        error = urllib.error.HTTPError(LOCATION, 503, "uncertain", {}, None)
        with (
            patch("urllib.request.urlopen", side_effect=error) as send,
            patch("fabric_medallion_deployer.fabric.time.sleep") as sleep,
            self.assertRaises(FabricApiError),
        ):
            self.client.submit_mlv_refresh(WS, LH)
        send.assert_called_once()
        sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
