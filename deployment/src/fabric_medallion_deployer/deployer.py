"""Registry-driven deployment with local validation before any cloud mutation."""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import re
import sys
import tempfile
import urllib.parse
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .auth import azure_cli_token
from .configuration import bind_configuration
from .contracts import validate_seed_data
from .definitions import (
    metadata_copy_pipeline_definition,
    metadata_master_pipeline_definition,
    validate_deployable_definition,
)
from .fabric import FabricApiError, FabricClient, FabricItem
from .gold import (
    gold_notebook_definition,
)
from .inventory import SourceBinding, reusable_inventory, runtime_source_bindings
from .metadata import ConnectionSpec
from .orchestrator import (
    METADATA_NOTEBOOKS,
    bootstrap_configuration,
    metadata_notebook_definition,
    orchestrator_database_definition,
)
from .use_cases import UseCaseProfile, load_use_case

LOGGER = logging.getLogger(__name__)
FOLDER_BY_TYPE = {
    "SQLDatabase": "database",
    "Lakehouse": "lakehouses",
    "Notebook": "notebooks",
    "DataPipeline": "pipelines",
}
ROUTE_BY_TYPE = {
    "SQLDatabase": "sqlDatabases",
    "Notebook": "notebooks",
    "DataPipeline": "dataPipelines",
}
METADATA_COPY_PIPELINE = "pl_copy_bronze_batch"
METADATA_MASTER_PIPELINE = "pl_bronze_silver_gold"


@dataclass(frozen=True)
class DeploymentOptions:
    workspace: str
    bas_sql_connection: str | None = None
    csd_sql_connection: str | None = None
    persal_sql_connection: str | None = None
    overwrite: bool = False
    dry_run: bool = False
    bootstrap_orchestrator: bool = False
    orchestrator_sql_connection: str = "conn_orchestrator_sql"
    interactive: bool = False
    state_file: Path | None = None
    resume: bool = False
    metadata_assets: bool = False
    use_case: str | None = None
    source_connections: dict[str, str] = field(default_factory=dict)


def _definition_digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def metadata_inventory(
    sources: tuple[SourceBinding, ...] = (), *, profile: UseCaseProfile | None = None
) -> dict[str, str]:
    """Derive the complete reusable inventory, including controlled MLV authoring."""
    if set(METADATA_NOTEBOOKS) != {
        "prepare",
        "silver",
        "worker",
        "gold",
        "gold_worker",
    }:
        raise ValueError(
            "Generic publication requires the complete Silver/Gold notebook compiler"
        )
    return reusable_inventory(sources, profile=profile)


def _metadata_definitions(
    root: Path,
    sources: tuple[SourceBinding, ...],
    workspace_id: str,
    item_ids: dict[str, str],
    control_connection_id: str,
    use_case: str | None = None,
) -> dict[str, dict[str, Any]]:
    profile = load_use_case(root, use_case)
    names = profile.notebook_names()
    copy_name, master_name = profile.pipeline_names()
    bronze, gold = profile.resources["bronze_silver"], profile.resources["gold"]
    definitions = {}
    for step, name in names.items():
        is_gold = step in {"gold", "gold_worker"}
        definitions[name] = metadata_notebook_definition(
            root,
            step,
            workspace_id=workspace_id,
            lakehouse_id=item_ids[gold if is_gold else bronze],
            lakehouse_name=gold if is_gold else bronze,
            use_case=profile.case_id,
            **(
                {
                    "source_workspace_id": workspace_id,
                    "source_lakehouse_id": item_ids[bronze],
                }
                if is_gold
                else {}
            ),
        )
    bindings = {
        "workspace_id": workspace_id,
        "control_database_id": item_ids[profile.resources["control_database"]],
        "control_connection_id": control_connection_id,
        "bronze_lakehouse_id": item_ids[bronze],
    }
    definitions[copy_name] = metadata_copy_pipeline_definition(
        root, **bindings, prepare_notebook_id=item_ids[names["prepare"]]
    )
    definitions[master_name] = metadata_master_pipeline_definition(
        root,
        **bindings,
        copy_pipeline_id=item_ids[copy_name],
        notebook_ids={
            step: item_ids[names[step]] for step in ("prepare", "silver", "gold")
        },
    )
    definitions[profile.mlv_notebook] = gold_notebook_definition(
        root,
        sources,
        "mlvs",
        workspace_id=workspace_id,
        gold_lakehouse_id=item_ids[gold],
        source_lakehouse_id=item_ids[bronze],
        use_case=profile.case_id,
    )
    for name, definition in definitions.items():
        validate_deployable_definition(definition)
        if definition.get("format") == "ipynb":
            notebook = json.loads(base64.b64decode(definition["parts"][0]["payload"]))
            for cell in notebook["cells"]:
                if cell["cell_type"] == "code":
                    compile("".join(cell["source"]), f"{name}/{cell['id']}", "exec")
    return definitions


def validate_metadata_local(
    root: Path, use_case: str | None = None
) -> tuple[SourceBinding, ...]:
    """Compile the opt-in assets only; no source seeds or legacy manifest checks."""
    profile = load_use_case(root, use_case)
    sources = runtime_source_bindings(root, profile.case_id)
    inventory = metadata_inventory(profile=profile)
    _metadata_definitions(
        root,
        sources,
        validation_id("workspace"),
        {name: validation_id(name) for name in inventory},
        validation_id("control-connection"),
        profile.case_id,
    )
    orchestrator_database_definition(
        root, bootstrap_configuration(root, use_case=profile.case_id)
    )
    return sources


class _PublicationJournal:
    """One explicit publication attempt; never retry an unreceipted submission."""

    def __init__(self, path: Path, state: dict[str, Any], client: FabricClient) -> None:
        self.path, self.state, self.client = path, state, client

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=self.path.parent,
            prefix=self.path.name + ".",
            suffix=".pending",
            delete=False,
        ) as stream:
            temporary = Path(stream.name)
            stream.write(json.dumps(self.state, sort_keys=True, indent=2) + "\n")
        try:
            temporary.replace(self.path)
        finally:
            temporary.unlink(missing_ok=True)

    @staticmethod
    def _validate_receipt(receipt: dict[str, str], request_path: str) -> None:
        location = urllib.parse.urlparse(receipt.get("operation_location", ""))
        host = location.hostname or ""
        if (
            receipt.get("method") != "POST"
            or receipt.get("request_path") != request_path
            or location.scheme != "https"
            or location.query
            or location.fragment
            or location.username
            or location.password
            or location.port not in {None, 443}
            or not (
                host == "api.fabric.microsoft.com"
                or (host.startswith("wabi-") and host.endswith(".analysis.windows.net"))
            )
            or re.fullmatch(r"/v1/operations/[A-Za-z0-9-]+", location.path) is None
        ):
            raise FabricApiError(
                "Publication receipt is untrusted or differs from its submitted operation"
            )

    def reconcile(self, *, dry_run: bool) -> None:
        """Check all uncertain work before allowing any subsequent mutation."""
        for key, operation in self.state["operations"].items():
            if (
                not isinstance(operation, dict)
                or operation.get("status") not in {"DONE", "SUBMITTING", "RECONCILED"}
                or not isinstance(operation.get("intent"), dict)
            ):
                raise ValueError("Invalid publication operation journal")
            if operation["status"] == "DONE":
                continue
            receipt = operation.get("receipt")
            if not receipt:
                raise FabricApiError(
                    f"Publication submission {key!r} is uncertain without a receipt; inspect service state before retry"
                )
            self._validate_receipt(receipt, operation["intent"]["request_path"])
            if dry_run:
                raise FabricApiError(
                    "Pending publication requires non-dry --resume reconciliation"
                )
            self.client._wait_for_operation(
                receipt["operation_location"], f"Resume {key}"
            )
            operation["status"] = "RECONCILED"
            self.save()

    def run(
        self,
        key: str,
        intent: dict[str, Any],
        submit: Callable[[], str],
        resolve: Callable[[], str],
    ) -> str:
        operations = self.state["operations"]
        operation = operations.get(key)
        if operation:
            if operation["status"] not in {"DONE", "RECONCILED"}:
                raise FabricApiError(
                    "Publication operation must be reconciled before reuse"
                )
            if operation["intent"] != intent:
                raise ValueError(
                    "Retained publication intent differs; do not rebind an operation"
                )
            identity = resolve()
            if operation.get("result_id") not in {None, identity}:
                raise FabricApiError(
                    "Published item identity differs from retained state"
                )
        else:
            operation = {"intent": intent, "status": "SUBMITTING", "receipt": None}
            operations[key] = operation
            self.save()

            def observe(receipt: dict[str, str]) -> None:
                self._validate_receipt(receipt, intent["request_path"])
                operation["receipt"] = receipt
                self.save()

            previous = self.client.operation_observer
            self.client.operation_observer = observe
            try:
                identity = submit()
            finally:
                self.client.operation_observer = previous
            if resolve() != identity:
                raise FabricApiError(
                    "Submitted operation has no uniquely attributable result"
                )
        operation.update(status="DONE", result_id=identity)
        self.save()
        return identity


def resolve_connection_prerequisites(
    client: FabricClient,
    selections: dict[str, str],
    *,
    requirements: Mapping[str, ConnectionSpec] | None = None,
    interactive: bool = False,
    prompt: Callable[[str], str] | None = None,
) -> dict[str, str]:
    """Report every missing/expired prerequisite before guided, user-controlled recheck."""
    if interactive and prompt is None and not sys.stdin.isatty():
        raise ValueError("Interactive connection setup requires a TTY")
    if requirements is not None and not set(requirements) <= selections.keys():
        raise ValueError("Connection requirements must match selected roles")
    while True:
        resolved, unavailable, failures = {}, [], []
        for role, selected in selections.items():
            try:
                spec = requirements.get(role) if requirements else None
                if spec is not None and spec.connector_type == "SqlServer":
                    connection = client.resolve_managed_connection(
                        selected,
                        "SQL",
                        credential_type=spec.authentication_type,
                        gateway_id=spec.gateway_id,
                        connectivity_types={
                            "ShareableCloud",
                            "OnPremisesGateway",
                            "VirtualNetworkGateway",
                        },
                    )
                elif spec is None or spec.connector_type == "FabricSql":
                    connection = client.resolve_oauth2_connection(selected, "FabricSql")
                else:
                    raise FabricApiError(
                        f"Unsupported publication connection: {spec.connector_type}"
                    )
                client.validate_connection_online(connection)
                resolved[role] = connection["id"]
            except FabricApiError as error:
                unavailable.append(selected)
                failures.append(str(error))
        if not unavailable:
            return resolved
        names = ", ".join(unavailable)
        message = (
            "Create/authorize or repair these managed source/control connections in "
            f"Manage connections and gateways: {names}."
        )
        if not interactive:
            raise FabricApiError(
                message + " " + "; ".join(failures) + " Re-run the command after setup."
            )
        answer = (
            (prompt or input)(message + " Enter recheck or cancel: ").strip().lower()
        )
        if answer not in {"", "recheck"}:
            raise ValueError(
                "Connection setup cancelled; retained bootstrap state is unchanged"
            )


def inventory_manifest(
    sources: tuple[SourceBinding, ...] = (), *, profile: UseCaseProfile | None = None
) -> dict[str, Any]:
    return {
        "solution": "Metadata medallion accelerator",
        "version": 3,
        "use_case": profile.case_id if profile else None,
        "items": [
            {"displayName": name, "type": kind}
            for name, kind in reusable_inventory(sources, profile=profile).items()
        ],
    }


def validation_id(name: str) -> str:
    """Return a deterministic test-only identity, never an actual deployment binding."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"medallion-local-validation/{name}"))


def validate_local(
    root: Path, *, validate_data: bool = True, use_case: str | None = None
) -> tuple[SourceBinding, ...]:
    """Compile and validate all owned items without acquiring a token."""
    profile = load_use_case(root, use_case)
    sources = runtime_source_bindings(root, profile.case_id)
    if validate_data and profile.adapter == "snapshot_bootstrap":
        validate_seed_data(root, use_case=profile.case_id)
    manifest_path = profile.generated / "deployment" / "manifest.json"
    if json.loads(manifest_path.read_text(encoding="utf-8")) != inventory_manifest(
        profile=profile
    ):
        raise ValueError("Stale deployment manifest; run deployment/build.py")
    _metadata_definitions(
        root,
        sources,
        validation_id("workspace"),
        {name: validation_id(name) for name in metadata_inventory(profile=profile)},
        validation_id("control-connection"),
        profile.case_id,
    )
    orchestrator_database_definition(
        root, bootstrap_configuration(root, use_case=profile.case_id)
    )
    return sources


class AcceleratorDeployer:
    def __init__(
        self,
        repository_root: Path,
        options: DeploymentOptions,
        *,
        client: FabricClient | None = None,
    ) -> None:
        self.root = repository_root
        self.options = options
        self.profile = load_use_case(repository_root, options.use_case)
        self.client = client or FabricClient(azure_cli_token)
        self.sources: tuple[SourceBinding, ...] = ()
        self.items: dict[str, FabricItem] = {}
        self.folders: dict[str, str] = {}

    def deploy(self) -> dict[str, FabricItem]:
        if self.options.metadata_assets or not self.options.bootstrap_orchestrator:
            if self.options.dry_run and self.options.interactive:
                raise ValueError("--dry-run is noninteractive")
            if self.options.resume and self.options.state_file is None:
                raise ValueError("--resume requires --state-file")
            if not self.options.dry_run and self.options.state_file is None:
                raise ValueError("--metadata-assets requires --state-file")
            return self._deploy_metadata_assets()
        if self.options.bootstrap_orchestrator:
            return self._deploy_orchestrator()
        raise ValueError("No compatible deployment mode selected")

    def _deploy_metadata_assets(self) -> dict[str, FabricItem]:
        """Publish an inactive control draft and reusable assets, never execute data."""
        profile = self.profile
        names = profile.notebook_names()
        copy_name, master_name = profile.pipeline_names()
        control_name = profile.resources["control_database"]
        self.sources = runtime_source_bindings(self.root, profile.case_id)
        if set(self.options.source_connections) - {
            source.code for source in self.sources
        }:
            raise ValueError(
                "Source connection override is not declared by the selected case"
            )
        inventory = metadata_inventory(profile=profile)
        config = bootstrap_configuration(self.root, use_case=profile.case_id)
        package = _metadata_definitions(
            self.root,
            self.sources,
            validation_id("workspace"),
            {name: validation_id(name) for name in inventory},
            validation_id("control-connection"),
            profile.case_id,
        )
        workspace = self.client.resolve_workspace(self.options.workspace)
        workspace_id = workspace["id"]
        capacities = {item["id"]: item for item in self.client.paged("/capacities")}
        if capacities.get(workspace.get("capacityId"), {}).get("state") != "Active":
            raise FabricApiError(
                "Generic publication requires active visible Fabric capacity"
            )

        def find(name: str, kind: str, *, required: bool = False) -> FabricItem | None:
            matches = [
                item
                for item in self.client.list_items(workspace_id)
                if item.display_name == name and item.type.casefold() != "sqlendpoint"
            ]
            if len(matches) > 1 or (
                matches and matches[0].type.casefold() != kind.casefold()
            ):
                raise FabricApiError(
                    f"{name!r} must resolve uniquely as a compatible {kind}"
                )
            if required and not matches:
                raise FabricApiError(f"{name!r} requires an existing compatible {kind}")
            return matches[0] if matches else None

        source_ids = {}
        for source in self.sources:
            if not source.database:
                continue
            item = find(source.database, "SQLDatabase", required=True)
            assert item is not None
            source_ids[source.code] = item.id
        if len(set(source_ids.values())) != len(source_ids):
            raise FabricApiError(
                "Source databases must resolve to distinct existing items"
            )
        existing = {name: find(name, kind) for name, kind in inventory.items()}
        for item in existing.values():
            if item and item.type.casefold() == "lakehouse":
                self._validate_lakehouse(workspace_id, item)
        existing_folders = self._folder_inventory(workspace_id)
        connections = resolve_connection_prerequisites(
            self.client,
            {
                **{
                    source.code: self.options.source_connections.get(
                        source.code,
                        getattr(self.options, f"{source.code}_sql_connection", None)
                        or source.connection,
                    )
                    for source in self.sources
                },
                "orchestrator": self.options.orchestrator_sql_connection,
            },
            requirements={
                feed.source_code: next(
                    connection
                    for connection in config.connections
                    if connection.id == feed.connection_id
                )
                for feed in config.feeds
            },
            interactive=self.options.interactive,
        )
        if len(set(connections.values())) != len(connections):
            raise FabricApiError(
                "Source and control connections must have distinct connection IDs"
            )
        for identity in (
            workspace_id,
            *source_ids.values(),
            *connections.values(),
            *(item.id for item in existing.values() if item),
            *existing_folders.values(),
        ):
            if str(uuid.UUID(identity)) != identity:
                raise ValueError(
                    "Generic publication bindings must be resolved canonical UUIDs"
                )
        config = bind_configuration(
            config,
            workspace_id=workspace_id,
            source_database_ids=source_ids,
            connection_ids={
                source.code: connections[source.code] for source in self.sources
            },
            version_id=f"{profile.case_id}-bound-"
            + _definition_digest(
                {
                    "definition_sha256": config.definition_sha256,
                    "workspace_id": workspace_id,
                    "source_database_ids": source_ids,
                    "connection_ids": connections,
                }
            )[:32],
        )
        control_definition = orchestrator_database_definition(self.root, config)
        package[control_name] = control_definition
        binding = {
            "format_version": 1,
            "mode": "metadata-assets",
            "use_case": profile.case_id,
            "workspace_id": workspace_id,
            "metadata_version_id": config.version_id,
            "package_sha256": _definition_digest(package),
            "source_database_ids": source_ids,
            "connection_ids": connections,
        }
        path = self.options.state_file
        if path is not None and path.exists():
            if not self.options.resume:
                raise ValueError(
                    "Publication state exists; use --resume with its state file"
                )
            state = json.loads(path.read_text(encoding="utf-8"))
            if (
                not isinstance(state, dict)
                or any(state.get(key) != value for key, value in binding.items())
                or not isinstance(state.get("operations"), dict)
                or not isinstance(state.get("resolved_items"), dict)
                or not isinstance(state.get("folder_ids"), dict)
            ):
                raise ValueError(
                    "Publication state target/package/bindings differ; do not rebind uncertain work"
                )
        elif self.options.resume:
            raise FileNotFoundError("The requested publication state file is missing")
        else:
            state = {
                **binding,
                "operations": {},
                "folder_ids": {},
                "resolved_items": {
                    name: item.id for name, item in existing.items() if item
                },
            }
        for name, identity in state["resolved_items"].items():
            if (
                name not in existing
                or existing[name] is None
                or existing[name].id != identity
            ):
                raise FabricApiError(
                    "Published item identity differs from retained state"
                )
        for name, identity in state["folder_ids"].items():
            if self._folder_inventory(workspace_id).get(name) != identity:
                raise FabricApiError(
                    "Published folder identity differs from retained state"
                )
        journal = _PublicationJournal(
            path.resolve() if path else Path(), state, self.client
        )
        # Reconcile before overwrite checks: a submitted create may now be visible.
        journal.reconcile(dry_run=self.options.dry_run)
        for name, item in existing.items():
            if (
                item
                and (
                    item.type.casefold() != "lakehouse"
                    or item.folder_id != existing_folders.get("lakehouses")
                    or "lakehouses" not in existing_folders
                )
                and f"create:{name}" not in state["operations"]
                and f"update:{name}" not in state["operations"]
                and f"move:{name}" not in state["operations"]
                and not self.options.overwrite
            ):
                raise FabricApiError(
                    f"{name!r} already exists; explicit --overwrite is required"
                )
        ids = {
            name: item.id if item else validation_id(name)
            for name, item in existing.items()
        }
        _metadata_definitions(
            self.root, self.sources, workspace_id, ids, connections["orchestrator"]
        )
        if self.options.dry_run:
            LOGGER.info(
                "Generic asset preflight passed; no publication, activation or jobs performed"
            )
            return {}
        journal.save()

        def item_id(name: str, kind: str, folder: str | None = None) -> str:
            item = find(name, kind, required=True)
            assert item is not None
            if folder is not None and item.folder_id != folder:
                raise FabricApiError(
                    f"Published item {name!r} is not in its expected folder"
                )
            return item.id

        def folder_id(name: str) -> str:
            identity = self._folder_inventory(workspace_id).get(name)
            if identity is None:
                raise FabricApiError(f"Published folder {name!r} is missing")
            return identity

        for name in FOLDER_BY_TYPE.values():
            key = f"folder:{name}"
            identity = self._folder_inventory(workspace_id).get(name)
            if identity is None or key in state["operations"]:
                identity = journal.run(
                    key,
                    {
                        "request_path": f"/workspaces/{workspace_id}/folders",
                        "name": name,
                    },
                    lambda name=name: self.client.create_folder(workspace_id, name),
                    lambda name=name: folder_id(name),
                )
            self.folders[name] = identity
            state["folder_ids"][name] = identity
            journal.save()

        def ensure(
            name: str, kind: str, definition: dict[str, Any] | None = None
        ) -> FabricItem:
            folder = self.folders[FOLDER_BY_TYPE[kind]]
            item = find(name, kind)
            retained = state["resolved_items"].get(name)
            if retained and (item is None or item.id != retained):
                raise FabricApiError(
                    "Published item identity changed during publication"
                )
            create_key = f"create:{name}"
            created = create_key in state["operations"] or item is None
            if (
                item
                and not created
                and (definition is not None or item.folder_id != folder)
                and f"update:{name}" not in state["operations"]
                and f"move:{name}" not in state["operations"]
                and not self.options.overwrite
            ):
                raise FabricApiError(
                    f"{name!r} already exists; explicit --overwrite is required"
                )
            route = "lakehouses" if kind == "Lakehouse" else ROUTE_BY_TYPE[kind]
            if created:
                payload = (
                    {"enableSchemas": True}
                    if kind == "Lakehouse"
                    else {
                        "collation": "Latin1_General_100_BIN2_UTF8",
                        "creationMode": "new",
                    }
                    if kind == "SQLDatabase"
                    else None
                )
                create_definition = definition if payload is None else None
                journal.run(
                    create_key,
                    {
                        "request_path": f"/workspaces/{workspace_id}/{route}",
                        "name": name,
                        "folder_id": folder,
                        "payload": payload,
                        "definition_sha256": _definition_digest(create_definition),
                    },
                    lambda: (
                        self.client.create_item(
                            workspace_id,
                            route,
                            name,
                            folder_id=folder,
                            creation_payload=payload,
                            definition=create_definition,
                        ).id
                    ),
                    lambda: item_id(name, kind),
                )
                item = find(name, kind, required=True)
            assert item is not None
            state["resolved_items"][name] = item.id
            journal.save()
            if definition is not None and (not created or kind == "SQLDatabase"):

                def update() -> str:
                    self.client.update_definition(workspace_id, item.id, definition)
                    return item.id

                journal.run(
                    f"update:{name}",
                    {
                        "request_path": f"/workspaces/{workspace_id}/items/{item.id}/updateDefinition",
                        "item_id": item.id,
                        "definition_sha256": _definition_digest(definition),
                    },
                    update,
                    lambda: item_id(name, kind),
                )
            move_key = f"move:{name}"
            if item.folder_id != folder or move_key in state["operations"]:

                def move() -> str:
                    self.client.move_items(workspace_id, [item.id], folder)
                    return item.id

                journal.run(
                    move_key,
                    {
                        "request_path": f"/workspaces/{workspace_id}/items/bulkMove",
                        "item_id": item.id,
                        "folder_id": folder,
                    },
                    move,
                    lambda: item_id(name, kind, folder),
                )
            item = find(name, kind, required=True)
            assert item is not None
            self.items[name] = item
            ids[name] = item.id
            return item

        ensure(control_name, "SQLDatabase", control_definition)
        for name in (profile.resources["bronze_silver"], profile.resources["gold"]):
            lakehouse = ensure(name, "Lakehouse")
            self._validate_lakehouse(workspace_id, lakehouse)
        # Compile each dependent slice again with the actual persisted predecessor IDs.
        for name in (
            *names.values(),
            profile.mlv_notebook,
            copy_name,
            master_name,
        ):
            definition = _metadata_definitions(
                self.root,
                self.sources,
                workspace_id,
                ids,
                connections["orchestrator"],
                profile.case_id,
            )[name]
            ensure(name, inventory[name], definition)
        for name, kind in inventory.items():
            item_id(name, kind, self.folders[FOLDER_BY_TYPE[kind]])
        LOGGER.info(
            "Generic assets published with a bound inactive control draft; source seeds, activation and jobs unchanged. "
            "Native acceptance and explicit validation/approval/activation remain pending."
        )
        return self.items

    def _deploy_orchestrator(self) -> dict[str, FabricItem]:
        """Bootstrap the additive control database without reseeding any source."""
        config = bootstrap_configuration(self.root, use_case=self.profile.case_id)
        control_name = self.profile.resources["control_database"]
        definition = orchestrator_database_definition(self.root, config)
        digest = hashlib.sha256(
            json.dumps(definition, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
        ).hexdigest()
        workspace = self.client.resolve_workspace(self.options.workspace)
        workspace_id = workspace["id"]
        capacities = {item["id"]: item for item in self.client.paged("/capacities")}
        if capacities.get(workspace.get("capacityId"), {}).get("state") != "Active":
            raise FabricApiError(
                "The control database requires active visible Fabric capacity"
            )
        matches = [
            item
            for item in self.client.list_items(workspace_id)
            if item.display_name == control_name
            and item.type.casefold() != "sqlendpoint"
        ]
        if len(matches) > 1 or (matches and matches[0].type != "SQLDatabase"):
            raise FabricApiError(
                "db_orchestrator must resolve to one compatible SQLDatabase"
            )
        existing = matches[0] if matches else None
        if self.options.dry_run:
            resolve_connection_prerequisites(
                self.client, {"orchestrator": self.options.orchestrator_sql_connection}
            )
            LOGGER.info(
                "Control-only dry-run passed; no schema, data or workspace items changed"
            )
            return {}
        path = self.options.state_file
        if path is None:
            raise ValueError(
                "Control bootstrap requires --state-file for persistent operation reconciliation"
            )
        path = path.resolve()
        if path.exists():
            if not self.options.resume:
                raise ValueError(
                    "Bootstrap state exists; use --resume with its state file"
                )
            state = json.loads(path.read_text(encoding="utf-8"))
            if (
                not isinstance(state, dict)
                or state.get("workspace_id") != workspace_id
                or state.get("definition_sha256") != digest
                or state.get("metadata_version_id") != config.version_id
            ):
                raise ValueError(
                    "Bootstrap state target/package differs; do not rebind an uncertain operation"
                )
        elif self.options.resume:
            raise FileNotFoundError("The requested bootstrap state file is missing")
        else:
            state = {
                "format_version": 1,
                "workspace_id": workspace_id,
                "definition_sha256": digest,
                "metadata_version_id": config.version_id,
                "database_id": existing.id if existing else None,
                "step": "NEW",
                "pending_operation": None,
            }

        def save() -> None:
            path.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=path.parent,
                prefix=path.name + ".",
                suffix=".pending",
                delete=False,
            ) as stream:
                temporary = Path(stream.name)
                stream.write(json.dumps(state, sort_keys=True, indent=2) + "\n")
            try:
                temporary.replace(path)
            finally:
                temporary.unlink(missing_ok=True)

        def observe(receipt: dict[str, str]) -> None:
            location = urllib.parse.urlparse(receipt["operation_location"])
            if (
                location.query
                or location.fragment
                or location.username
                or (
                    location.hostname != "api.fabric.microsoft.com"
                    and not (location.hostname or "").endswith(".analysis.windows.net")
                )
            ):
                raise FabricApiError(
                    "Bootstrap receipt contains an untrusted or credential-bearing location"
                )
            state["pending_operation"] = receipt
            save()

        pending = state.get("pending_operation")
        if pending:
            self.client._wait_for_operation(
                pending["operation_location"], "Resume control bootstrap"
            )
            state["pending_operation"] = None
            state["step"] = (
                "DATABASE_RESOLVED"
                if state["step"] == "CREATING_DATABASE"
                else "SCHEMA_READY"
            )
            existing = self.client.find_item(workspace_id, control_name, "SQLDatabase")
            if existing is None:
                raise FabricApiError(
                    "Completed control operation has no attributable database item"
                )
            state["database_id"] = existing.id
            save()
        elif state["step"] in {"CREATING_DATABASE", "UPDATING_SCHEMA"}:
            raise FabricApiError(
                "Bootstrap submission is uncertain without a receipt; inspect service state before retry"
            )
        previous_observer = self.client.operation_observer
        self.client.operation_observer = observe
        try:
            if state["step"] == "NEW":
                if existing and not self.options.overwrite:
                    raise FabricApiError(
                        "db_orchestrator already exists; explicit --overwrite is required for schema update"
                    )
                if existing is None:
                    state["step"] = "CREATING_DATABASE"
                    save()
                    existing = self.client.create_item(
                        workspace_id,
                        "sqlDatabases",
                        control_name,
                        description="Metadata medallion control database",
                        creation_payload={
                            "collation": "Latin1_General_100_BIN2_UTF8",
                            "creationMode": "new",
                        },
                        folder_id=None,
                    )
                    state["database_id"] = existing.id
                state["pending_operation"] = None
                state["step"] = "DATABASE_RESOLVED"
                save()
            if existing is None or existing.id != state["database_id"]:
                raise FabricApiError(
                    "Control database identity differs from retained bootstrap state"
                )
            if state["step"] == "DATABASE_RESOLVED":
                state["step"] = "UPDATING_SCHEMA"
                save()
                self.client.update_definition(workspace_id, existing.id, definition)
                state["pending_operation"] = None
                state["step"] = "SCHEMA_READY"
                save()
            connections = resolve_connection_prerequisites(
                self.client,
                {"orchestrator": self.options.orchestrator_sql_connection},
                interactive=self.options.interactive,
            )
            state["control_connection_id"] = connections["orchestrator"]
            state["step"] = "CONNECTION_READY"
            save()
            self.items[control_name] = existing
            LOGGER.info(
                "Control bootstrap complete with an inactive draft; no source seeds, activation, schedules or jobs changed"
            )
            return self.items
        finally:
            self.client.operation_observer = previous_observer

    def _expected_items(self) -> dict[str, str]:
        return metadata_inventory(profile=self.profile)

    def _check_conflicts(self, workspace_id: str) -> None:
        existing = self.client.list_items(workspace_id)
        for name, expected_type in self._expected_items().items():
            matches = [item for item in existing if item.display_name == name]
            incompatible = [
                i
                for i in matches
                if i.type.casefold() not in {expected_type.casefold(), "sqlendpoint"}
            ]
            if incompatible:
                raise FabricApiError(
                    f"{name!r} is occupied by another type; expected {expected_type}"
                )
            compatible = [
                i for i in matches if i.type.casefold() == expected_type.casefold()
            ]
            if len(compatible) > 1:
                raise FabricApiError(
                    f"Multiple {expected_type} items are named {name!r}"
                )
            if compatible and not self.options.overwrite:
                raise FabricApiError(
                    f"{name!r} already exists; use --overwrite for compatible items"
                )

    def _folder_inventory(self, workspace_id: str) -> dict[str, str]:
        existing = self.client.list_folders(workspace_id)
        folders = {}
        for name in FOLDER_BY_TYPE.values():
            matches = [
                f
                for f in existing
                if f["displayName"] == name and not f.get("parentFolderId")
            ]
            if len(matches) > 1:
                raise FabricApiError(f"Multiple root folders are named {name!r}")
            if matches:
                folders[name] = matches[0]["id"]
        return folders

    def _ensure_folders(self, workspace_id: str) -> dict[str, str]:
        existing = self._folder_inventory(workspace_id)
        return {
            name: existing[name]
            if name in existing
            else self.client.create_folder(workspace_id, name)
            for name in FOLDER_BY_TYPE.values()
        }

    def _validate_lakehouse(self, workspace_id: str, item: FabricItem) -> None:
        details = self.client.request(
            "GET", f"/workspaces/{workspace_id}/lakehouses/{item.id}", expected=(200,)
        ).json()
        if not details.get("properties", {}).get("defaultSchema"):
            raise FabricApiError(
                f"Existing Lakehouse {item.display_name!r} is not schema-enabled"
            )

    def _ensure_lakehouse(self, workspace_id: str, name: str) -> FabricItem:
        existing = self.client.find_item(workspace_id, name, "Lakehouse")
        folder = self.folders["lakehouses"]
        if existing:
            self._validate_lakehouse(workspace_id, existing)
            self.client.move_items(workspace_id, [existing.id], folder)
            item = existing
        else:
            item = self.client.create_item(
                workspace_id,
                "lakehouses",
                name,
                description="Fabric medallion Lakehouse",
                creation_payload={"enableSchemas": True},
                folder_id=folder,
            )
        self.items[name] = item
        LOGGER.info("Resolved Lakehouse: %s (%s)", name, item.id)
        return item

    def _ensure_definition_item(
        self,
        workspace_id: str,
        *,
        item_type: str,
        display_name: str,
        definition: dict[str, Any],
        creation_payload: dict[str, Any] | None = None,
    ) -> FabricItem:
        validate_deployable_definition(definition)
        existing = self.client.find_item(workspace_id, display_name, item_type)
        folder = self.folders[FOLDER_BY_TYPE[item_type]]
        if existing:
            if not self.options.overwrite:
                raise FabricApiError(f"{display_name!r} already exists")
            self.client.update_definition(workspace_id, existing.id, definition)
            self.client.move_items(workspace_id, [existing.id], folder)
            item = existing
        else:
            item = self.client.create_item(
                workspace_id,
                ROUTE_BY_TYPE[item_type],
                display_name,
                description=f"Fabric medallion {item_type}",
                definition=definition if creation_payload is None else None,
                creation_payload=creation_payload,
                folder_id=folder,
            )
            if creation_payload is not None:
                self.client.update_definition(workspace_id, item.id, definition)
        self.items[display_name] = item
        LOGGER.info("Resolved %s: %s (%s)", item_type, display_name, item.id)
        return item

    def _validate_inventory(self, workspace_id: str) -> None:
        items = self.client.list_items(workspace_id)
        for name, item_type in self._expected_items().items():
            matches = [
                i
                for i in items
                if i.display_name == name and i.type.casefold() == item_type.casefold()
            ]
            if len(matches) != 1:
                raise FabricApiError(
                    f"Expected exactly one deployed {item_type}: {name}"
                )
            if matches[0].folder_id != self.folders[FOLDER_BY_TYPE[item_type]]:
                raise FabricApiError(f"Deployed item is in the wrong folder: {name}")
