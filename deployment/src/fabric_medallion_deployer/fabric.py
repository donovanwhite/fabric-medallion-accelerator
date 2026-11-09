"""Microsoft Fabric REST client used by the reusable accelerator deployer."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

FABRIC_API = "https://api.fabric.microsoft.com/v1"


class FabricApiError(RuntimeError):
    """Raised when a Fabric REST operation fails."""


@dataclass(frozen=True)
class FabricItem:
    """Minimal Fabric item identity."""

    id: str
    display_name: str
    type: str
    folder_id: str | None = None


@dataclass(frozen=True)
class ApiResponse:
    """HTTP response data used by the client."""

    status_code: int
    headers: Any
    content: bytes

    def json(self) -> dict[str, Any]:
        """Decode the JSON response body."""
        return json.loads(self.content.decode("utf-8")) if self.content else {}


class FabricClient:
    """Small Fabric REST client with pagination and LRO handling."""

    def __init__(
        self,
        token_provider: Callable[[], str],
        *,
        timeout_seconds: int = 120,
        lro_timeout_seconds: int = 1800,
        operation_observer: Callable[[dict[str, str]], None] | None = None,
    ) -> None:
        self.token_provider = token_provider
        self.timeout_seconds = timeout_seconds
        self.lro_timeout_seconds = lro_timeout_seconds
        self.operation_observer = operation_observer

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token_provider()}",
            "Content-Type": "application/json",
            "x-ms-fabric-skill": "spark-cli",
        }

    def request(
        self,
        method: str,
        path_or_url: str,
        *,
        body: dict[str, Any] | None = None,
        expected: tuple[int, ...] = (200,),
    ) -> ApiResponse:
        """Send one request and raise an actionable error on failure."""
        url = (
            path_or_url
            if path_or_url.startswith("https://")
            else f"{FABRIC_API}{path_or_url}"
        )
        data = json.dumps(body).encode("utf-8") if body is not None else None
        for attempt in range(5):
            request = urllib.request.Request(
                url,
                data=data,
                headers=self._headers(),
                method=method,
            )
            try:
                with urllib.request.urlopen(
                    request, timeout=self.timeout_seconds
                ) as response:
                    result = ApiResponse(
                        status_code=response.status,
                        headers=response.headers,
                        content=response.read(),
                    )
                break
            except urllib.error.HTTPError as error:
                retryable = method.upper() == "GET" and (
                    error.code == 429 or error.code >= 500
                )
                if retryable and attempt < 4:
                    delay = int(error.headers.get("Retry-After", "5").split(",")[0])
                    time.sleep(max(delay, 1))
                    continue
                detail = error.read().decode("utf-8", errors="replace").strip()
                raise FabricApiError(
                    f"{method} {url} failed with HTTP {error.code}: {detail}"
                ) from error
        else:
            raise FabricApiError(f"{method} {url} exhausted retry attempts")
        if result.status_code not in expected:
            detail = result.content.decode("utf-8", errors="replace").strip()
            raise FabricApiError(
                f"{method} {url} failed with HTTP {result.status_code}: {detail}"
            )
        return result

    def request_lro(
        self,
        method: str,
        path: str,
        *,
        body: dict[str, Any] | None = None,
        synchronous: tuple[int, ...] = (200, 201),
    ) -> dict[str, Any]:
        """Send a Fabric operation and wait for an accepted LRO."""
        response = self.request(
            method,
            path,
            body=body,
            expected=(*synchronous, 202),
        )
        if response.status_code in synchronous:
            return response.json()

        location = response.headers.get("Location")
        if not location:
            raise FabricApiError(f"{method} {path} returned 202 without Location")
        if self.operation_observer is not None:
            self.operation_observer(
                {"method": method, "request_path": path, "operation_location": location}
            )
        return self._wait_for_operation(location, f"{method} {path}")

    def _wait_for_operation(self, location: str, operation_name: str) -> dict[str, Any]:
        """Wait for one Fabric long-running operation."""
        parsed = urllib.parse.urlparse(location)
        if (
            parsed.hostname
            and parsed.hostname.startswith("wabi-")
            and parsed.hostname.endswith(".analysis.windows.net")
            and parsed.path.startswith("/v1/operations/")
        ):
            location = "https://api.fabric.microsoft.com" + parsed.path
        delay = 5
        deadline = time.monotonic() + self.lro_timeout_seconds
        while time.monotonic() < deadline:
            time.sleep(max(delay, 1))
            poll = self.request("GET", location, expected=(200,))
            payload = poll.json()
            status = payload.get("status")
            if status == "Succeeded":
                return payload
            if status == "Failed":
                raise FabricApiError(
                    f"{operation_name} failed: {payload.get('error', payload)}"
                )
            delay = int(poll.headers.get("Retry-After", str(delay)).split(",")[0])
        raise FabricApiError(f"{operation_name} did not complete before timeout")

    def paged(self, path: str) -> list[dict[str, Any]]:
        """Return every value from a paginated Fabric collection."""
        results: list[dict[str, Any]] = []
        next_url: str | None = path
        while next_url:
            payload = self.request("GET", next_url, expected=(200,)).json()
            results.extend(payload.get("value", []))
            next_url = payload.get("continuationUri")
        return results

    def resolve_workspace(self, name_or_id: str) -> dict[str, Any]:
        """Resolve exactly one existing workspace."""
        if len(name_or_id) == 36 and name_or_id.count("-") == 4:
            return self.request(
                "GET", f"/workspaces/{name_or_id}", expected=(200,)
            ).json()
        matches = [
            workspace
            for workspace in self.paged("/workspaces")
            if workspace.get("displayName") == name_or_id
        ]
        if len(matches) != 1:
            raise FabricApiError(
                f"Expected one workspace named {name_or_id!r}; found {len(matches)}"
            )
        return self.request(
            "GET", f"/workspaces/{matches[0]['id']}", expected=(200,)
        ).json()

    def resolve_oauth2_connection(
        self, name_or_id: str, connection_type: str
    ) -> dict[str, Any]:
        """Resolve one OAuth2 cloud connection by name and type."""
        return self.resolve_managed_connection(
            name_or_id,
            connection_type,
            credential_type="OAuth2",
            connectivity_types={"ShareableCloud"},
        )

    def resolve_managed_connection(
        self,
        name_or_id: str,
        connection_type: str,
        *,
        credential_type: str,
        connectivity_types: set[str],
        gateway_id: str | None = None,
    ) -> dict[str, Any]:
        """Resolve managed references without requesting or handling credentials."""
        try:
            connection_id = str(uuid.UUID(name_or_id))
        except ValueError:
            matches = [
                connection
                for connection in self.paged("/connections")
                if connection.get("displayName") == name_or_id
                and connection.get("connectionDetails", {}).get("type")
                == connection_type
            ]
            if len(matches) != 1:
                raise FabricApiError(
                    f"Expected one {connection_type} {credential_type} connection named "
                    f"{name_or_id!r}; found {len(matches)}. Create and authorize "
                    "the connection in Fabric, or pass its ID with the "
                    "corresponding --<source>-sql-connection option."
                )
            connection_id = matches[0]["id"]
        connection = self.request(
            "GET", f"/connections/{connection_id}", expected=(200,)
        ).json()
        actual_type = connection.get("connectionDetails", {}).get("type")
        if actual_type != connection_type:
            raise FabricApiError(
                f"Connection {name_or_id!r} must have type {connection_type}; "
                f"found {actual_type or 'no connection type'}"
            )
        if connection.get("connectivityType") not in connectivity_types:
            raise FabricApiError(
                f"Connection {name_or_id!r} must use {sorted(connectivity_types)} "
                f"connection; found {connection.get('connectivityType')}"
            )
        actual_credential = connection.get("credentialDetails", {}).get(
            "credentialType"
        )
        if actual_credential != credential_type:
            raise FabricApiError(
                f"Connection {name_or_id!r} must use {credential_type} credentials; "
                f"found {actual_credential or 'no credential type'}"
            )
        actual_gateway = connection.get("gatewayId")
        if connection.get("connectivityType") != "ShareableCloud":
            try:
                if (
                    not isinstance(actual_gateway, str)
                    or str(uuid.UUID(actual_gateway)) != actual_gateway
                ):
                    raise ValueError
            except ValueError:
                raise FabricApiError(
                    f"Connection {name_or_id!r} requires a resolved gateway reference"
                ) from None
        if gateway_id is not None and actual_gateway != gateway_id:
            raise FabricApiError(
                f"Connection {name_or_id!r} differs from its declared gateway reference"
            )
        return connection

    def validate_connection_online(self, connection: dict[str, Any]) -> None:
        """Fail before deployment when a connection is offline or expired."""
        connection_id = connection["id"]
        path = f"/connections/{connection_id}/testConnection"
        response = self.request("POST", path, expected=(200, 202))
        if response.status_code == 200:
            result = response.json()
        else:
            location = response.headers.get("Location")
            if not location:
                raise FabricApiError(f"POST {path} returned 202 without Location")
            self._wait_for_operation(location, f"Test connection {connection_id}")
            result = self.request("GET", f"{location}/result", expected=(200,)).json()
        if result.get("status") != "Online":
            errors = result.get("errors") or []
            details = "; ".join(error.get("message", str(error)) for error in errors)
            if not details:
                details = (
                    "Reauthorize or repair it in Manage connections and "
                    "gateways before deployment."
                )
            raise FabricApiError(
                f"Connection {connection.get('displayName', connection_id)!r} "
                f"is {result.get('status', 'not online')}. {details}"
            )

    def list_items(self, workspace_id: str) -> list[FabricItem]:
        """List generic workspace items."""
        return [
            FabricItem(
                id=item["id"],
                display_name=item["displayName"],
                type=item["type"],
                folder_id=item.get("folderId"),
            )
            for item in self.paged(f"/workspaces/{workspace_id}/items")
        ]

    def find_item(
        self, workspace_id: str, display_name: str, item_type: str
    ) -> FabricItem | None:
        """Find a unique item by display name and type."""
        matches = [
            item
            for item in self.list_items(workspace_id)
            if item.display_name == display_name
            and item.type.casefold() == item_type.casefold()
        ]
        if len(matches) > 1:
            raise FabricApiError(
                f"Multiple {item_type} items are named {display_name!r}"
            )
        return matches[0] if matches else None

    def create_item(
        self,
        workspace_id: str,
        route: str,
        display_name: str,
        *,
        description: str = "",
        definition: dict[str, Any] | None = None,
        creation_payload: dict[str, Any] | None = None,
        folder_id: str | None = None,
    ) -> FabricItem:
        """Create an item and resolve its identity after provisioning."""
        body: dict[str, Any] = {
            "displayName": display_name,
            "description": description,
        }
        if definition is not None:
            body["definition"] = definition
        if creation_payload is not None:
            body["creationPayload"] = creation_payload
        if folder_id is not None:
            body["folderId"] = folder_id
        self.request_lro(
            "POST",
            f"/workspaces/{workspace_id}/{route}",
            body=body,
            synchronous=(201,),
        )
        item_type = {
            "lakehouses": "Lakehouse",
            "sqlDatabases": "SQLDatabase",
            "notebooks": "Notebook",
            "dataPipelines": "DataPipeline",
        }[route]
        for _ in range(12):
            item = self.find_item(workspace_id, display_name, item_type)
            if item is not None:
                return item
            time.sleep(5)
        raise FabricApiError(f"Created {item_type} {display_name!r} was not found")

    def update_definition(
        self,
        workspace_id: str,
        item_id: str,
        definition: dict[str, Any],
    ) -> None:
        """Update one item definition."""
        self.request_lro(
            "POST",
            f"/workspaces/{workspace_id}/items/{item_id}/updateDefinition",
            body={"definition": definition},
            synchronous=(200,),
        )

    def list_folders(self, workspace_id: str) -> list[dict[str, Any]]:
        """List every workspace folder."""
        return self.paged(f"/workspaces/{workspace_id}/folders")

    def create_folder(self, workspace_id: str, display_name: str) -> str:
        """Create one root workspace folder and return its ID."""
        payload = self.request(
            "POST",
            f"/workspaces/{workspace_id}/folders",
            body={"displayName": display_name},
            expected=(201,),
        ).json()
        return payload["id"]

    def move_items(
        self, workspace_id: str, item_ids: list[str], folder_id: str
    ) -> None:
        """Move up to 50 items into a workspace folder."""
        self.request(
            "POST",
            f"/workspaces/{workspace_id}/items/bulkMove",
            body={"items": item_ids, "targetFolderId": folder_id},
            expected=(200,),
        )

    @staticmethod
    def _mlv_path(workspace_id: str, lakehouse_id: str) -> str:
        """Use the Lakehouse-owned MLV scheduler, not generic RunNotebook."""
        workspace_id = str(uuid.UUID(workspace_id))
        lakehouse_id = str(uuid.UUID(lakehouse_id))
        return f"/workspaces/{workspace_id}/lakehouses/{lakehouse_id}"

    def list_mlv_refresh_schedules(
        self, workspace_id: str, lakehouse_id: str
    ) -> list[dict[str, Any]]:
        """List full/selected-lineage schedules, including disabled schedules."""
        path = self._mlv_path(workspace_id, lakehouse_id)
        return self._mlv_collection(
            f"{path}/jobs/refreshMaterializedLakeViews/schedules"
        )

    def list_mlv_refresh_jobs(
        self, workspace_id: str, lakehouse_id: str
    ) -> list[dict[str, Any]]:
        """Read MLV history without unsupported OData filters."""
        path = self._mlv_path(workspace_id, lakehouse_id)
        return self._mlv_collection(
            f"{path}/jobs/refreshMaterializedLakeViews/instances"
        )

    def _mlv_collection(self, path: str) -> list[dict[str, Any]]:
        """Read all pages, including token-only MLV history responses."""
        results, seen = [], set()
        next_path: str | None = path
        while next_path:
            if next_path in seen:
                raise FabricApiError("Repeated MLV continuation page")
            seen.add(next_path)
            payload = self.request("GET", next_path).json()
            if not isinstance(payload.get("value"), list):
                raise FabricApiError("Malformed MLV collection response")
            results.extend(payload["value"])
            next_path = payload.get("continuationUri")
            token = payload.get("continuationToken")
            if next_path:
                parsed = urllib.parse.urlsplit(next_path)
                if (
                    parsed.scheme != "https"
                    or parsed.netloc != "api.fabric.microsoft.com"
                    or parsed.path != f"/v1{path}"
                    or parsed.fragment
                ):
                    raise FabricApiError("Untrusted MLV continuation URI")
            elif token:
                next_path = (
                    path + "?" + urllib.parse.urlencode({"continuationToken": token})
                )
        return results

    def get_mlv_execution_definition(
        self, workspace_id: str, lakehouse_id: str, definition_id: str
    ) -> dict[str, Any]:
        """Read a saved selected-lineage configuration; not a visual graph API."""
        path = self._mlv_path(workspace_id, lakehouse_id)
        return self.request(
            "GET",
            f"{path}/mlvexecutiondefinitions/{uuid.UUID(definition_id)}",
        ).json()

    def submit_mlv_refresh(
        self,
        workspace_id: str,
        lakehouse_id: str,
        *,
        execution_definition_id: str | None = None,
    ) -> dict[str, Any]:
        """Submit ONCE; caller must persist intent before calling this method.

        See the Lakehouse Run On Demand Refresh Materialized Lake Views REST
        reference. A missing/malformed receipt is uncertain, never permission to
        retry. No polling, POST retries or fabricated per-view mode are added.
        """
        path = self._mlv_path(workspace_id, lakehouse_id)
        body = (
            {
                "executionData": {
                    "mlvExecutionDefinitionId": str(uuid.UUID(execution_definition_id))
                }
            }
            if execution_definition_id is not None
            else None
        )
        response = self.request(
            "POST",
            f"{path}/jobs/refreshMaterializedLakeViews/instances",
            body=body,
            expected=(202,),
        )
        location = response.headers.get("Location", "")
        parsed = urllib.parse.urlsplit(location)
        # Both Location shapes are documented. Do not send a bearer token to
        # arbitrary response-provided hosts, paths, or another Lakehouse.
        allowed = (
            (
                f"/v1/workspaces/{uuid.UUID(workspace_id)}/items/"
                f"{uuid.UUID(lakehouse_id)}/jobs/instances/"
            ),
            f"/v1{path}/jobs/instances/",
        )
        prefix = next((p for p in allowed if parsed.path.startswith(p)), None)
        if (
            parsed.scheme != "https"
            or parsed.netloc != "api.fabric.microsoft.com"
            or parsed.query
            or parsed.fragment
            or prefix is None
        ):
            raise FabricApiError("MLV submission receipt has no trusted job Location")
        try:
            job_id = str(uuid.UUID(parsed.path[len(prefix) :]))
            delay = int(response.headers["Retry-After"])
            if delay < 0:
                raise ValueError("negative delay")
        except (ValueError, KeyError) as error:
            raise FabricApiError("MLV submission receipt is incomplete") from error
        return {"job_id": job_id, "location": location, "retry_after": delay}

    def get_mlv_refresh_job(
        self, workspace_id: str, lakehouse_id: str, job_id: str
    ) -> dict[str, Any]:
        """Read the documented item-scoped status; status is lineage-level."""
        self._mlv_path(workspace_id, lakehouse_id)
        return self.request(
            "GET",
            f"/workspaces/{uuid.UUID(workspace_id)}/items/"
            f"{uuid.UUID(lakehouse_id)}/jobs/instances/{uuid.UUID(job_id)}",
        ).json()

    def cancel_mlv_refresh_job(
        self, workspace_id: str, lakehouse_id: str, job_id: str
    ) -> None:
        """Request cancellation once; cancellation is not completion evidence."""
        self._mlv_path(workspace_id, lakehouse_id)
        self.request(
            "POST",
            f"/workspaces/{uuid.UUID(workspace_id)}/items/"
            f"{uuid.UUID(lakehouse_id)}/jobs/instances/{uuid.UUID(job_id)}/cancel",
            expected=(202,),
        )
