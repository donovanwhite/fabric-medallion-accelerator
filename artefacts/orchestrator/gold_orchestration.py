"""Gold notebook coordination using pinned manifests and persisted object results.

The control, Gold and ingestion modules are injected from the hashed worker
bundle. This module has no repository imports or mutable configuration reads.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any
from uuid import UUID


class SourceFileSystem:
    """Resolve relative metadata paths in Bronze from a Gold-bound notebook."""

    def __init__(self, fs: Any, workspace_id: str, lakehouse_id: str) -> None:
        for value in (workspace_id, lakehouse_id):
            try:
                valid = isinstance(value, str) and str(UUID(value)) == value
            except ValueError:
                valid = False
            if not valid:
                raise ValueError(
                    "Gold source bindings require resolved canonical UUIDs"
                )
        self.fs = fs
        self.root = (
            f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}"
        )

    def _path(self, path: str, *, write: bool = False) -> str:
        if not isinstance(path, str):
            raise TypeError("Source storage paths must be strings")
        relative = path.removeprefix(self.root + "/")
        if (
            not relative.startswith("Files/")
            or "\\" in relative
            or any(part in {"", ".", ".."} for part in relative.split("/"))
            or (write and not relative.startswith("Files/orchestration/"))
        ):
            raise ValueError("Gold source access escapes its bound storage scope")
        return self.root + "/" + relative

    def exists(self, path: str) -> bool:
        return self.fs.exists(self._path(path))

    def ls(self, path: str) -> Any:
        return self.fs.ls(self._path(path))

    def head(self, path: str, max_bytes: int) -> str:
        return self.fs.head(self._path(path), max_bytes)

    def mkdirs(self, path: str) -> Any:
        return self.fs.mkdirs(self._path(path, write=True))

    def put(self, path: str, contents: str, overwrite: bool = False) -> bool:
        if overwrite:
            raise ValueError("Gold orchestration metadata is immutable")
        return self.fs.put(self._path(path, write=True), contents, False)


def source_filesystem(
    fs: Any, workspace_id: str, lakehouse_id: str
) -> SourceFileSystem:
    return SourceFileSystem(fs, workspace_id, lakehouse_id)


class MlvJournalFileSystem:
    """Allow journal updates only in the fenced MLV control directory."""

    def __init__(self, source: SourceFileSystem) -> None:
        self.source = source

    def exists(self, path: str) -> bool:
        return self.source.exists(path)

    def head(self, path: str, max_bytes: int) -> str:
        return self.source.head(path, max_bytes)

    def mkdirs(self, path: str) -> Any:
        if path != "Files/orchestration/mlv_lifecycle":
            raise ValueError("MLV journal directory differs from its fixed scope")
        return self.source.mkdirs(path)

    def put(self, path: str, contents: str, overwrite: bool = False) -> bool:
        if not path.startswith("Files/orchestration/mlv_lifecycle/"):
            raise ValueError("MLV journal update escapes its control scope")
        return self.source.fs.put(
            self.source._path(path, write=True), contents, overwrite
        )


def _configuration(manifest: Any, gold: Any, code_sha256: str, lakehouse: str) -> dict:
    return gold.gold_config_from_manifest(
        manifest.to_document(),
        snapshot_sha256=manifest.snapshot_sha256,
        code_sha256=code_sha256,
        lakehouse=lakehouse,
    )


def _checked_dispatch(control: Any, manifest: Any, dispatch: Any) -> None:
    if dispatch.layer != "gold":
        raise RuntimeError("Gold requires its prepared Gold layer dispatch")
    checked = control.make_layer_dispatch(
        manifest, "gold", dispatch.to_document()["attempts"]
    )
    if checked.dispatch_sha256 != dispatch.dispatch_sha256:
        raise RuntimeError("Gold dispatch differs from its pinned worker context")


def _preparation_path(manifest: Any, dispatch: Any) -> str:
    return (
        f"Files/orchestration/{manifest.run_id}/gold/"
        f"preparation_{dispatch.dispatch_sha256}.json"
    )


def _identity(manifest: Any, dispatch: Any, code_sha256: str) -> dict:
    return {
        "format_version": 1,
        "run_id": manifest.run_id,
        "version_id": manifest.version_id,
        "snapshot_sha256": manifest.snapshot_sha256,
        "dispatch_sha256": dispatch.dispatch_sha256,
        "code_sha256": code_sha256,
    }


def _read_preparation(
    fs: Any, manifest: Any, dispatch: Any, code_sha256: str, control: Any
) -> dict:
    value = control._read_json_metadata(
        fs, _preparation_path(manifest, dispatch), control.MAX_MANIFEST_BYTES
    )
    expected = _identity(manifest, dispatch, code_sha256)
    if set(value) != {*expected, "preparation"} or any(
        value[key] != item for key, item in expected.items()
    ):
        raise RuntimeError("Gold preparation differs from its pinned dispatch")
    if value["preparation"].get("gold_release_id") != manifest.run_id:
        raise RuntimeError("Gold preparation belongs to another release")
    return value["preparation"]


def _stage_path(manifest: Any, task_id: str, attempt: Mapping[str, Any]) -> str:
    return (
        f"Files/orchestration/{manifest.run_id}/gold/"
        f"{task_id}/{attempt['attempt_id']}.json"
    )


def _stage_identity(
    manifest: Any, dispatch: Any, code_sha256: str, task_id: str, control: Any
) -> dict:
    attempt = control._dispatch_attempt(dispatch, task_id)
    return {
        **_identity(manifest, dispatch, code_sha256),
        "task_id": task_id,
        "attempt_id": attempt["attempt_id"],
        "fence": attempt["fence"],
        "source_fence": attempt["source_fence"],
    }


def _read_result(
    spark: Any,
    fs: Any,
    manifest: Any,
    dispatch: Any,
    config: dict,
    preparation: dict,
    name: str,
    *,
    control: Any,
    gold: Any,
) -> tuple[dict, dict]:
    task = config["_metadata"]["tasks"][name]
    attempt = control._dispatch_attempt(dispatch, task["task_id"])
    stage = control._read_json_metadata(
        fs, _stage_path(manifest, task["task_id"], attempt), control.MAX_RESULT_BYTES
    )
    identity = _stage_identity(
        manifest, dispatch, config["runtime_sha256"], task["task_id"], control
    )
    if (
        set(stage) != {*identity, "status", "result", "error_code"}
        or any(stage.get(key) != value for key, value in identity.items())
        or stage["status"] != "SUCCEEDED"
        or stage["error_code"] is not None
        or not isinstance(stage["result"], dict)
        or stage["result"].get("attempt_id") != attempt["attempt_id"]
        or stage["result"].get("fence") != attempt["fence"]
    ):
        raise RuntimeError("Gold staged result is failed, incomplete or rebound")
    result = gold.load_gold_result(spark, config, preparation, name, stage["result"])
    if result["output"]["table"] != task["target"]:
        raise RuntimeError("Gold result does not belong to its declared target")
    return stage["result"], result


def execute_gold_task(
    spark: Any,
    fs: Any,
    manifest: Any,
    dispatch: Any,
    *,
    task_id: str,
    worker_id: str,
    code_sha256: str,
    lakehouse: str,
    preparation_json: str,
    runtime_context: Mapping[str, Any],
    control: Any,
    gold: Any,
    ingestion: Any,
) -> str:
    job_id = runtime_context.get("currentRunId")
    if runtime_context.get("isReferenceRun") is not True or not job_id:
        raise RuntimeError(
            "Gold workers require an actual native reference run identity"
        )
    control._token(job_id, "Gold reference job identity", r"[A-Za-z0-9_-]{1,128}")
    resolved = control.resolve_worker_task(
        manifest,
        task_id,
        worker_id=worker_id,
        code_sha256=code_sha256,
        lakehouse=lakehouse,
    )
    if resolved["task"]["layer"] != "gold":
        raise RuntimeError("Gold worker cannot execute another layer")
    _checked_dispatch(control, manifest, dispatch)
    config = _configuration(manifest, gold, code_sha256, lakehouse)
    preparation = _read_preparation(fs, manifest, dispatch, code_sha256, control)
    if json.loads(preparation_json) != preparation:
        raise RuntimeError(
            "Gold worker preparation differs from its persisted reference"
        )
    names = [
        name
        for name, task in config["_metadata"]["tasks"].items()
        if task["task_id"] == task_id
    ]
    if len(names) != 1:
        raise RuntimeError("Gold task is missing or ambiguous")
    name = names[0]
    dependencies = {
        parent: _read_result(
            spark,
            fs,
            manifest,
            dispatch,
            config,
            preparation,
            parent,
            control=control,
            gold=gold,
        )[0]
        for parent in config["_metadata"]["tasks"][name]["dependencies"]
    }
    attempt = control._dispatch_attempt(dispatch, task_id)
    path = _stage_path(manifest, task_id, attempt)
    identity = _stage_identity(manifest, dispatch, code_sha256, task_id, control)
    if fs.exists(path):
        reference, _result = _read_result(
            spark,
            fs,
            manifest,
            dispatch,
            config,
            preparation,
            name,
            control=control,
            gold=gold,
        )
        if reference["job_id"] != job_id:
            raise RuntimeError(
                "A retained Gold attempt cannot be rebound to a new reference run"
            )
        return path
    try:
        reference = gold.build_gold_object(
            spark,
            fs,
            config,
            preparation,
            name,
            ingestion,
            attempt_id=attempt["attempt_id"],
            fence=attempt["fence"],
            job_id=job_id,
            code_sha256=code_sha256,
            dependency_results=dependencies,
        )
    except ingestion._operation_errors():
        control._write_immutable(
            fs,
            path,
            control._canonical(
                {
                    **identity,
                    "status": "FAILED",
                    "result": None,
                    "error_code": "GOLD_OBJECT_FAILED",
                }
            ),
            control.MAX_RESULT_BYTES,
        )
        raise RuntimeError(
            "Gold object failed; release readiness is withheld"
        ) from None
    control._write_immutable(
        fs,
        path,
        control._canonical(
            {
                **identity,
                "status": "SUCCEEDED",
                "result": reference,
                "error_code": None,
            }
        ),
        control.MAX_RESULT_BYTES,
    )
    return path


def run_gold_parent(
    spark: Any,
    fs: SourceFileSystem,
    manifest: Any,
    pipeline: Any,
    *,
    manifest_path: str,
    lakehouse: str,
    code_sha256: str,
    concurrency: int,
    runner: Any,
    control: Any,
    gold: Any,
    ingestion: Any,
    load_configuration: Any,
    file_arrivals: Any = None,
) -> dict:
    if type(concurrency) is not int or not 1 <= concurrency <= 16:
        raise ValueError("Gold DAG concurrency must be between 1 and 16")
    dispatch = control.notebook_layer_dispatch(manifest, pipeline)
    _checked_dispatch(control, manifest, dispatch)
    config = _configuration(manifest, gold, code_sha256, lakehouse)
    dispatch_path = control.write_layer_dispatch(fs, dispatch)
    by_feed = control.prepare_silver_contexts(
        fs,
        manifest,
        load_configuration=load_configuration,
        file_arrivals=file_arrivals,
    )
    references, runs = {}, {}
    for source in config["sources"]:
        reference = by_feed[source["source_feed_id"]]
        context = ingestion.manifest_processing_context(
            control._read_bounded(fs, reference["path"], control.MAX_MANIFEST_BYTES),
            reference["sha256"],
            expected_lakehouse=source["lakehouse"],
        )
        references[source["source_code"]] = reference
        runs[source["source_code"]] = {
            "pipeline_run_id": context["pipeline_run_id"],
            "processing_date": context["processing_date"].isoformat(),
        }
    preparation = gold.prepare_gold(
        spark,
        fs,
        config,
        json.dumps(runs),
        manifest.run_id,
        ingestion,
        fs.root + "/Files/bronze",
        source_contexts=references,
    )
    control._write_immutable(
        fs,
        _preparation_path(manifest, dispatch),
        control._canonical(
            {
                **_identity(manifest, dispatch, code_sha256),
                "preparation": preparation,
            }
        ),
        control.MAX_MANIFEST_BYTES,
    )
    try:
        dag = gold.build_gold_dag(
            config,
            preparation,
            manifest_path=manifest_path,
            dispatch_path=dispatch_path,
            dispatch_sha256=dispatch.dispatch_sha256,
            lakehouse=lakehouse,
            concurrency=concurrency,
        )
        control.run_notebook_dag(runner, dag, dispatch)
        results = {
            name: _read_result(
                spark,
                fs,
                manifest,
                dispatch,
                config,
                preparation,
                name,
                control=control,
                gold=gold,
            )[0]
            for name in config["_metadata"]["tasks"]
        }
        gold.finalize_gold(spark, config, preparation, results)
    except Exception:
        gold.fail_gold_release(spark, config, preparation)
        raise
    return {
        "run_id": manifest.run_id,
        "gold_release_id": manifest.run_id,
        "status": "FACTS_READY",
        "objects": len(results),
    }


def reconcile_gold_pipeline(
    spark: Any,
    fs: Any,
    manifest: Any,
    pipeline: Any,
    *,
    client: Any,
    observed_utc: str,
    lakehouse: str,
    code_sha256: str,
    control: Any,
    gold: Any,
    ingestion: Any,
) -> dict:
    evidence = control.pipeline_activity_evidence(
        client,
        manifest,
        pipeline,
        observed_utc=observed_utc,
        parent_activity="orchestrate_gold",
    )
    dispatch = control.notebook_layer_dispatch(manifest, pipeline)
    _checked_dispatch(control, manifest, dispatch)
    config = _configuration(manifest, gold, code_sha256, lakehouse)
    failure = None
    objects, release = {}, None
    if any(value["observed_job_status"] != "Completed" for value in evidence.values()):
        failure = "GOLD_PARENT_FAILED"
    else:
        try:
            preparation = _read_preparation(
                fs, manifest, dispatch, code_sha256, control
            )
            references = {}
            for name, task in config["_metadata"]["tasks"].items():
                reference, result = _read_result(
                    spark,
                    fs,
                    manifest,
                    dispatch,
                    config,
                    preparation,
                    name,
                    control=control,
                    gold=gold,
                )
                references[name] = reference
                objects[task["task_id"]] = result["output"]
            gold.finalize_gold(spark, config, preparation, references)
            table = f"{config['target_lakehouse']}.{config['schema']}.ctl_gold_release"
            release = {
                "kind": "gold_release",
                "location": table,
                "version": ingestion.delta_version(spark, table),
                "sha256": preparation["input_sha256"],
            }
        except ingestion._operation_errors():
            failure = "GOLD_OUTPUT_OR_RELEASE_INVALID"
    results = []
    for attempt in pipeline.to_document()["attempts"]:
        service = evidence[attempt["task_id"]]
        refs, metrics = [], {}
        if failure is None:
            output = objects[attempt["task_id"]]
            refs = [
                {
                    "kind": "delta_version",
                    "location": output["table"],
                    "version": output["version"],
                    "sha256": gold._gold_digest(output),
                },
                release,
            ]
        result = control.make_task_result(
            manifest,
            attempt["task_id"],
            attempt_id=attempt["attempt_id"],
            fence=attempt["fence"],
            job_id=service["job_id"],
            status="SUCCEEDED" if failure is None else "FAILED",
            finished_utc=service["finished_utc"],
            artifact_refs=refs,
            metrics=metrics,
            readiness=["succeeded", "facts_ready"] if failure is None else [],
            error_code=failure,
        )
        control.persist_task_result(fs, result)
        results.append((result, service))
    result = {
        "run_id": manifest.run_id,
        "has_failed": failure is not None,
        "callbacks": control.write_pipeline_callbacks(fs, manifest, results),
    }
    report = {
        **_identity(manifest, dispatch, code_sha256),
        "has_failed": result["has_failed"],
        "task_ids": sorted(evidence),
        "callbacks": result["callbacks"],
    }
    text = control._canonical(report)
    digest = gold._gold_digest(report)
    path = f"Files/orchestration/{manifest.run_id}/gold/reconciliation/{digest}.json"
    control._write_immutable(fs, path, text, control.MAX_MANIFEST_BYTES)
    return {
        **result,
        "gold_reconciliation_path": path,
        "gold_reconciliation_sha256": digest,
    }


def require_gold_success(
    fs: Any,
    manifest: Any,
    pipeline: Any,
    *,
    path: str,
    expected_sha256: str,
    code_sha256: str,
    control: Any,
    gold: Any,
) -> dict:
    dispatch = control.notebook_layer_dispatch(manifest, pipeline)
    _checked_dispatch(control, manifest, dispatch)
    value = control._read_json_metadata(
        fs, control._manifest_path(path, manifest.run_id), control.MAX_MANIFEST_BYTES
    )
    identity = _identity(manifest, dispatch, code_sha256)
    if (
        gold._gold_digest(value) != expected_sha256
        or set(value) != {*identity, "has_failed", "task_ids", "callbacks"}
        or any(value[key] != item for key, item in identity.items())
        or value["task_ids"]
        != sorted(attempt["task_id"] for attempt in dispatch.to_document()["attempts"])
        or value["has_failed"] is not False
    ):
        raise RuntimeError(
            "Gold reconciliation failed or changed; readiness is withheld"
        )
    return {"run_id": manifest.run_id, "status": "GOLD_RESULTS_RECONCILED"}


def mlv_for_release(
    spark: Any,
    fs: Any,
    manifest: Any,
    *,
    gold: Any,
    mlv: Any,
    code_sha256: str,
    lakehouse: str,
    client: Any,
    workspace_id: str,
    lakehouse_id: str,
    execution_definition_id: str,
    release_id: str,
    action: str,
    ownership_check: Any,
    attempt_id: str = "",
    mode_evidence: Any = None,
) -> dict:
    """Execute an explicit MLV lifecycle under an independently verified writer fence.

    The integrating coordinator supplies a real ownership checker, not a Boolean
    parameter or a default no-op. No runtime is inferred from local compilation.
    """
    if not callable(ownership_check):
        raise TypeError(
            "MLV execution requires an authoritative serving ownership checker"
        )
    config = _configuration(manifest, gold, code_sha256, lakehouse)
    if action not in {"author_mlvs", "refresh_mlvs"}:
        raise ValueError("Unknown MLV lifecycle operation")
    release = gold._ready_release(spark, config, release_id)
    if release is None or release["contract_hash"] != config["contract_hash"]:
        raise RuntimeError("MLV execution requires its exact FACTS_READY Gold release")
    retained = dict(release)
    runtime = mlv.SparkMlvRuntime(
        spark,
        schema_enabled=lambda: bool(
            client.request(
                "GET",
                f"/workspaces/{workspace_id}/lakehouses/{lakehouse_id}",
                expected=(200,),
            )
            .json()
            .get("properties", {})
            .get("defaultSchema")
        ),
    )
    definitions = mlv.definitions_from_metadata(manifest.to_document()["configuration"])
    if not definitions:
        raise RuntimeError(
            "No approved enabled MLV definitions belong to this operation"
        )
    outputs = json.loads(release["output_manifest_json"])
    inputs = {
        entry["table"]: {key: entry[key] for key in ("table", "location", "version")}
        for entry in outputs.values()
        if entry.get("table")
        in {table for definition in definitions for table in definition.inputs}
    }

    def unchanged_release() -> None:
        current = gold._ready_release(spark, config, release_id)
        if current != retained:
            raise RuntimeError("Gold serving release changed during MLV execution")

    lifecycle = mlv.MlvLifecycle(
        mlv.NotebookStateStore(
            MlvJournalFileSystem(fs) if isinstance(fs, SourceFileSystem) else fs,
            "Files/orchestration/mlv_lifecycle",
        ),
        runtime,
        mlv.FabricMlvJobs(
            client,
            workspace_id,
            lakehouse_id,
            execution_definition_id,
            lakehouse_name=lakehouse,
        ),
        ownership_check=ownership_check,
        release={
            key: release[key]
            for key in (
                "gold_release_id",
                "status",
                "contract_hash",
                "input_manifest_json",
                "output_manifest_json",
            )
        }
        | {"execution_mode": config["execution_mode"]},
        release_check=unchanged_release,
    )
    if action == "author_mlvs":
        for definition in definitions:
            lifecycle.register(
                definition,
                {table: inputs[table] for table in definition.inputs},
                approved_sha256=definition.sha256,
                approval_id=manifest.version_id,
            )
        return {
            "run_id": manifest.run_id,
            "status": "MLV_DEFINITIONS_REGISTERED",
            "definitions": len(definitions),
        }
    if not attempt_id or not callable(mode_evidence):
        raise RuntimeError(
            "Refresh requires a stable attempt identity and retained actual-mode observer"
        )
    return lifecycle.refresh(
        attempt_id, definitions, inputs, observe_mode=mode_evidence
    )
