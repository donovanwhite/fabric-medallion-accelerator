"""Bounded immutable run-manifest transport, embedded into generic notebooks.

Metadata validation is injected from the same pure module used by deployment.
No connection credentials, repository imports or mutable active-version reads
are required by this runtime.
"""

from __future__ import annotations

import csv
import fnmatch
import hashlib
import io
import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePosixPath
from typing import Any, Literal, Protocol, cast
from uuid import UUID

MAX_MANIFEST_BYTES = 16 * 1024 * 1024
MAX_RESULT_BYTES = 1024 * 1024
csv.field_size_limit(max(csv.field_size_limit(), MAX_MANIFEST_BYTES))


class FileInfo(Protocol):
    name: str
    size: int


class FileSystem(Protocol):
    def exists(self, path: str) -> bool: ...
    def ls(self, path: str) -> Sequence[FileInfo]: ...
    def head(self, path: str, max_bytes: int) -> str: ...
    def mkdirs(self, path: str) -> object: ...
    def put(self, path: str, contents: str, overwrite: bool = False) -> bool: ...


class PinnedConfiguration(Protocol):
    version_id: str

    @property
    def definition_sha256(self) -> str: ...


class SnapshotPlan(Protocol):
    run_id: str
    snapshot_json: str

    @property
    def snapshot_sha256(self) -> str: ...


class NotebookRunner(Protocol):
    def validateDAG(self, dag: dict[str, object]) -> bool: ...
    def runMultiple(self, dag: dict[str, object]) -> dict[str, object]: ...


class SilverRuntime(Protocol):
    def read_raw(
        self, spark: Any, fs: Any, context: dict[str, Any], table: dict[str, Any]
    ) -> Any: ...
    def snapshot_fingerprint(self, frame: Any) -> tuple[str, int]: ...
    def manifest_processing_context(
        self,
        context_json: str,
        expected_context_sha256: str,
        *,
        expected_lakehouse: str,
    ) -> dict[str, Any]: ...
    def setup_namespaces(self, spark: Any, context: dict[str, Any]) -> None: ...
    def inventory(
        self,
        spark: Any,
        fs: Any,
        context: dict[str, Any],
        *,
        table_names: list[str] | None = None,
    ) -> None: ...
    def load_silver(
        self,
        spark: Any,
        fs: Any,
        context: dict[str, Any],
        *,
        table_names: list[str] | None = None,
    ) -> list[dict[str, Any]]: ...
    def validate_source(self, spark: Any, fs: Any, context: dict[str, Any]) -> None: ...
    def delta_version(self, spark: Any, target: str) -> int: ...
    def source_commit_evidence(
        self, spark: Any, context: dict[str, Any]
    ) -> dict[str, Any]: ...
    def cleanup_snapshots(
        self, spark: Any, fs: Any, context: dict[str, Any]
    ) -> None: ...
    def _operation_errors(self) -> tuple[type[Exception], ...]: ...


class RestResponse(Protocol):
    content: bytes


class ActivityClient(Protocol):
    def request(
        self,
        method: str,
        path_or_url: str,
        *,
        body: dict[str, Any],
        expected: tuple[int, ...],
    ) -> RestResponse: ...


@dataclass(frozen=True)
class VerifiedRunManifest:
    run_id: str
    version_id: str
    snapshot_json: str
    snapshot_sha256: str

    def to_document(self) -> dict[str, object]:
        return cast(dict[str, object], json.loads(self.snapshot_json))


def _canonical(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _manifest_path(path: str, run_id: str) -> str:
    if not isinstance(run_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", run_id):
        raise RuntimeError("Invalid run identity for manifest access")
    if not isinstance(path, str) or "\\" in path or ":" in path:
        raise RuntimeError("Manifest path must be a run-scoped relative OneLake path")
    parts = path.split("/")
    if any(
        part in {"", ".", ".."}
        or re.fullmatch(r"[A-Za-z0-9_-][A-Za-z0-9_.-]{0,255}", part) is None
        for part in parts
    ):
        raise RuntimeError("Manifest path cannot escape its run scope")
    if not path.startswith(f"Files/orchestration/{run_id}/"):
        raise RuntimeError("Manifest path belongs to another run or location")
    return path


def _read_bounded(fs: FileSystem, path: str, max_bytes: int) -> str:
    if type(max_bytes) is not int or not 1 <= max_bytes <= MAX_MANIFEST_BYTES:
        raise RuntimeError("Invalid manifest size bound")
    if not fs.exists(path):
        raise FileNotFoundError("Pinned metadata manifest is missing")
    location = PurePosixPath(path)
    entries = [
        item for item in fs.ls(str(location.parent)) if item.name == location.name
    ]
    if len(entries) != 1:
        raise RuntimeError("Manifest file membership is missing or ambiguous")
    size = entries[0].size
    if type(size) is not int or not 1 <= size <= max_bytes:
        raise RuntimeError("Manifest size exceeds the configured bound or is empty")
    text = fs.head(path, size + 1)
    if len(text.encode("utf-8")) != size:
        raise RuntimeError(
            "Manifest read was truncated or the file changed during access"
        )
    return text.removeprefix("\ufeff")


def _write_immutable(fs: FileSystem, path: str, text: str, max_bytes: int) -> str:
    if len(text.encode("utf-8")) > max_bytes:
        raise RuntimeError("Immutable artifact exceeds its supported size bound")
    if fs.exists(path):
        if _read_bounded(fs, path, max_bytes) != text:
            raise RuntimeError("An immutable artifact cannot be changed or overwritten")
        return path
    fs.mkdirs(str(PurePosixPath(path).parent))
    if fs.put(path, text, False) is not True:
        raise RuntimeError("Immutable artifact persistence was not acknowledged")
    if _read_bounded(fs, path, max_bytes) != text:
        raise RuntimeError("Immutable artifact readback differs after persistence")
    return path


def write_run_manifest(fs: FileSystem, plan: SnapshotPlan) -> str:
    path = _manifest_path(
        f"Files/orchestration/{plan.run_id}/manifest.json", plan.run_id
    )
    if (
        hashlib.sha256(plan.snapshot_json.encode("ascii")).hexdigest()
        != plan.snapshot_sha256
    ):
        raise RuntimeError("Run snapshot changed before persistence")
    if len(plan.snapshot_json.encode("ascii")) > MAX_MANIFEST_BYTES:
        raise RuntimeError(
            "Manifest size requires an explicitly supported chunked contract"
        )
    return _write_immutable(fs, path, plan.snapshot_json, MAX_MANIFEST_BYTES)


def read_run_manifest(
    fs: FileSystem,
    path: str,
    *,
    expected_snapshot_sha256: str,
    run_id: str,
    version_id: str,
    load_configuration: Callable[[object], PinnedConfiguration],
    export_column: str | None = None,
    max_bytes: int = MAX_MANIFEST_BYTES,
) -> VerifiedRunManifest:
    """Read JSON or a single-row Copy CSV export without a 64 KiB truncation."""
    path = _manifest_path(path, run_id)
    if not isinstance(expected_snapshot_sha256, str) or not re.fullmatch(
        r"[0-9a-f]{64}", expected_snapshot_sha256
    ):
        raise RuntimeError("An explicit manifest SHA-256 hash is required")
    text = _read_bounded(fs, path, max_bytes)
    if export_column is not None:
        reader = csv.DictReader(io.StringIO(text), escapechar="\\")
        if reader.fieldnames != [export_column]:
            raise RuntimeError(
                "Metadata export columns differ from the pinned contract"
            )
        rows = list(reader)
        if len(rows) != 1 or None in rows[0] or rows[0][export_column] is None:
            raise RuntimeError("Metadata export must contain one complete snapshot row")
        text = rows[0][export_column]
    try:
        payload = json.loads(text)
        canonical = _canonical(payload)
    except (ValueError, TypeError):
        raise RuntimeError("Pinned run manifest is not valid finite JSON") from None
    digest = hashlib.sha256(canonical.encode("ascii")).hexdigest()
    if digest != expected_snapshot_sha256:
        raise RuntimeError("Pinned run manifest hash mismatch")
    if not isinstance(payload, dict) or type(payload.get("format_version")) is not int:
        raise RuntimeError("Unsupported run manifest format")
    if payload["format_version"] != 1:
        raise RuntimeError("Unsupported run manifest format")
    if payload.get("run_id") != run_id or payload.get("version_id") != version_id:
        raise RuntimeError("Manifest run/version identity differs from worker context")
    if payload.get("can_execute") is not True or payload.get("blockers") != []:
        raise RuntimeError(
            "A preview or blocked manifest is not ready for worker execution"
        )
    configuration = load_configuration(payload.get("configuration"))
    if (
        configuration.version_id != version_id
        or configuration.definition_sha256 != payload.get("definition_sha256")
    ):
        raise RuntimeError("Pinned configuration definition hash or version differs")
    tasks = payload.get("tasks")
    if not isinstance(tasks, list) or not tasks:
        raise RuntimeError("Pinned run manifest has no task membership")
    if any(not isinstance(task, dict) or "task_id" not in task for task in tasks):
        raise RuntimeError("Invalid pinned task membership")
    identities = [task["task_id"] for task in tasks]
    if any(not isinstance(identity, str) for identity in identities) or len(
        set(identities)
    ) != len(identities):
        raise RuntimeError("Invalid or duplicate pinned task identity")
    return VerifiedRunManifest(run_id, version_id, canonical, digest)


def resolve_worker_task(
    manifest: VerifiedRunManifest,
    task_id: str,
    *,
    worker_id: str,
    code_sha256: str,
    lakehouse: str,
) -> dict[str, object]:
    """Resolve a detached task only for the declared code and Lakehouse binding."""
    payload = manifest.to_document()
    tasks = cast(list[dict[str, object]], payload["tasks"])
    matches = [task for task in tasks if task["task_id"] == task_id]
    if len(matches) != 1 or matches[0].get("worker_id") != worker_id:
        raise RuntimeError("Task is not assigned to this pinned worker")
    configuration = cast(dict[str, object], payload["configuration"])
    workers = cast(list[dict[str, object]], configuration["workers"])
    worker_matches = [worker for worker in workers if worker["id"] == worker_id]
    if len(worker_matches) != 1:
        raise RuntimeError("Pinned worker definition is missing or ambiguous")
    worker = worker_matches[0]
    if worker["code_sha256"] != code_sha256 or worker["lakehouse"] != lakehouse:
        raise RuntimeError(
            "Worker implementation or default Lakehouse differs from the snapshot"
        )
    objects = cast(list[dict[str, object]], configuration["objects"])
    obj = next((o for o in objects if o["id"] == matches[0]["object_id"]), None)
    if obj is None or obj["lakehouse"] != lakehouse or obj["worker_id"] != worker_id:
        raise RuntimeError("Pinned object binding differs from the task/worker")
    return {
        "task": matches[0],
        "object": obj,
        "rules": [
            rule
            for rule in cast(list[dict[str, object]], configuration["rules"])
            if rule["object_id"] == obj["id"]
        ],
    }


def _token(value: object, label: str, pattern: str) -> str:
    if not isinstance(value, str) or re.fullmatch(pattern, value) is None:
        raise RuntimeError(f"Invalid {label}")
    return value


def _attempt_identity(value: object) -> str:
    text = _token(value, "attempt identity", r"[0-9a-f-]{36}")
    try:
        identity = UUID(text)
    except ValueError:
        raise RuntimeError("Invalid attempt identity") from None
    if str(identity) != text:
        raise RuntimeError("Attempt identity must be a canonical UUID")
    return text


def _positive_fence(value: object) -> int:
    if type(value) is not int or not 1 <= value <= 2**63 - 1:
        raise RuntimeError("A positive signed bigint fence is required")
    return value


@dataclass(frozen=True)
class VerifiedTaskResult:
    run_id: str
    task_id: str
    attempt_id: str
    fence: int
    job_id: str
    status: str
    result_json: str

    @property
    def result_sha256(self) -> str:
        return hashlib.sha256(self.result_json.encode("ascii")).hexdigest()

    def to_document(self) -> dict[str, object]:
        return cast(dict[str, object], json.loads(self.result_json))


def load_task_result(result_json: str) -> VerifiedTaskResult:
    """Validate metadata-only result evidence; never accept raw exception text."""
    if (
        not isinstance(result_json, str)
        or len(result_json.encode("utf-8")) > MAX_RESULT_BYTES
    ):
        raise RuntimeError("Task result exceeds its bounded metadata contract")
    try:
        value = json.loads(result_json)
        canonical = _canonical(value)
    except (ValueError, TypeError):
        raise RuntimeError("Task result must be finite JSON") from None
    fields = {
        "format_version",
        "run_id",
        "task_id",
        "source_batch_id",
        "attempt_id",
        "fence",
        "job_id",
        "status",
        "finished_utc",
        "artifact_refs",
        "readiness",
        "error_code",
        "metrics",
    }
    if not isinstance(value, dict) or set(value) != fields or canonical != result_json:
        raise RuntimeError("Task result fields or canonical encoding differ")
    if type(value["format_version"]) is not int or value["format_version"] != 1:
        raise RuntimeError("Unsupported task result format")
    run_id = _token(value["run_id"], "run identity", r"[A-Za-z0-9_-]{1,128}")
    task_id = _token(value["task_id"], "task identity", r"[0-9a-f]{64}")
    attempt_id = _attempt_identity(value["attempt_id"])
    fence = _positive_fence(value["fence"])
    job_id = _token(value["job_id"], "service job identity", r"[A-Za-z0-9_-]{1,128}")
    if value["source_batch_id"] is not None:
        _token(value["source_batch_id"], "source batch identity", r"[0-9a-f]{64}")
    if not isinstance(value["status"], str) or value["status"] not in {
        "SUCCEEDED",
        "FAILED",
        "CANCELLED",
    }:
        raise RuntimeError("Task result status must be explicitly terminal")
    finished = _token(
        value["finished_utc"],
        "UTC completion instant",
        r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?Z",
    )
    try:
        datetime.fromisoformat(finished)
    except ValueError:
        raise RuntimeError("Invalid UTC completion instant") from None
    success = value["status"] == "SUCCEEDED"
    if success:
        if value["error_code"] is not None:
            raise RuntimeError("A successful result cannot contain a failure code")
    else:
        _token(value["error_code"], "failure code", r"[A-Z][A-Z0-9_]{0,127}")
    refs = value["artifact_refs"]
    if not isinstance(refs, list) or len(refs) > 1000 or (success and not refs):
        raise RuntimeError("Successful results require bounded artifact evidence")
    kinds = set()
    for ref in refs:
        if not isinstance(ref, dict) or set(ref) != {
            "kind",
            "location",
            "version",
            "sha256",
        }:
            raise RuntimeError("Invalid artifact fields; credentials are prohibited")
        if not isinstance(ref["kind"], str) or ref["kind"] not in {
            "raw_batch",
            "delta_version",
            "source_commit",
            "gold_release",
        }:
            raise RuntimeError("Unknown artifact evidence kind")
        kinds.add(ref["kind"])
        location = _token(
            ref["location"], "artifact location", r"[A-Za-z0-9_][A-Za-z0-9_./-]{0,1999}"
        )
        if any(part in {"", ".", ".."} for part in location.split("/")):
            raise RuntimeError("Artifact location cannot escape its declared scope")
        _token(ref["sha256"], "artifact evidence hash", r"[0-9a-f]{64}")
        if ref["kind"] == "raw_batch":
            if not location.startswith("Files/bronze/") or ref["version"] is not None:
                raise RuntimeError("Raw artifact must name a Bronze file batch")
        elif (
            re.fullmatch(
                r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){2}", location
            )
            is None
            or type(ref["version"]) is not int
            or not 0 <= ref["version"] <= 2**63 - 1
        ):
            raise RuntimeError(
                "Delta artifact requires a qualified table and exact version"
            )
    readiness = value["readiness"]
    if (
        not isinstance(readiness, list)
        or any(not isinstance(item, str) for item in readiness)
        or len(set(readiness)) != len(readiness)
        or not set(readiness)
        <= {"succeeded", "source_committed", "facts_ready", "gold_released"}
        or (success and "succeeded" not in readiness)
        or (not success and readiness)
    ):
        raise RuntimeError("Result readiness cannot turn a failure into success")
    for condition, kind in (
        ("source_committed", "source_commit"),
        ("facts_ready", "delta_version"),
        ("gold_released", "gold_release"),
    ):
        if condition in readiness and kind not in kinds:
            raise RuntimeError(
                "Publication readiness requires matching artifact evidence"
            )
    metrics = value["metrics"]
    if (
        not isinstance(metrics, dict)
        or not set(metrics) <= {"rows_read", "rows_written", "rows_quarantined"}
        or any(type(n) is not int or not 0 <= n <= 2**63 - 1 for n in metrics.values())
    ):
        raise RuntimeError("Only bounded row-count metrics are allowed")
    return VerifiedTaskResult(
        run_id, task_id, attempt_id, fence, job_id, value["status"], canonical
    )


def make_task_result(
    manifest: VerifiedRunManifest,
    task_id: str,
    *,
    attempt_id: str,
    fence: int,
    job_id: str,
    status: str,
    finished_utc: str,
    artifact_refs: Sequence[dict[str, object]] = (),
    readiness: Sequence[str] | None = None,
    error_code: str | None = None,
    metrics: dict[str, int] | None = None,
) -> VerifiedTaskResult:
    tasks = cast(list[dict[str, object]], manifest.to_document()["tasks"])
    matches = [task for task in tasks if task["task_id"] == task_id]
    if len(matches) != 1:
        raise RuntimeError("Result task identity is outside the pinned manifest")
    task = matches[0]
    payload = {
        "format_version": 1,
        "run_id": manifest.run_id,
        "task_id": task_id,
        "source_batch_id": task["source_batch_id"],
        "attempt_id": attempt_id,
        "fence": fence,
        "job_id": job_id,
        "status": status,
        "finished_utc": finished_utc,
        "artifact_refs": list(artifact_refs),
        "readiness": list(readiness)
        if readiness is not None
        else (["succeeded"] if status == "SUCCEEDED" else []),
        "error_code": error_code,
        "metrics": metrics if metrics is not None else {},
    }
    result = load_task_result(_canonical(payload))
    for ref in cast(list[dict[str, object]], result.to_document()["artifact_refs"]):
        if ref["kind"] in {"raw_batch", "delta_version"}:
            location, target = str(ref["location"]), str(task["target_path"])
            if location != target and not (
                ref["kind"] == "raw_batch" and location.startswith(target + "/")
            ):
                raise RuntimeError(
                    "Artifact evidence differs from the pinned task target"
                )
    return result


def _result_path(result: VerifiedTaskResult) -> str:
    return _manifest_path(
        f"Files/orchestration/{result.run_id}/results/"
        f"{result.task_id}/{result.attempt_id}.json",
        result.run_id,
    )


def persist_task_result(
    fs: FileSystem,
    result: VerifiedTaskResult,
    *,
    ingest: Callable[[str, str], object] | None = None,
) -> str:
    """Persist first, then reconcile SQL; a failed ingestion leaves replayable evidence."""
    result = load_task_result(result.result_json)
    path = _result_path(result)
    _write_immutable(fs, path, result.result_json, MAX_RESULT_BYTES)
    if ingest is not None:
        ingest(result.result_json, result.result_sha256)
    return path


def read_task_result(
    fs: FileSystem,
    path: str,
    *,
    run_id: str,
    task_id: str,
    attempt_id: str,
    expected_sha256: str,
) -> VerifiedTaskResult:
    text = _read_bounded(fs, _manifest_path(path, run_id), MAX_RESULT_BYTES)
    result = load_task_result(text)
    if (
        (result.run_id, result.task_id, result.attempt_id)
        != (run_id, task_id, attempt_id)
        or result.result_sha256 != expected_sha256
        or path != _result_path(result)
    ):
        raise RuntimeError("Task result identity, hash or immutable location differs")
    return result


@dataclass(frozen=True)
class RecordedAttempt:
    run_id: str
    task_id: str
    attempt_id: str
    fence: int
    status: str
    job_id: str | None

    def __post_init__(self) -> None:
        _token(self.run_id, "run identity", r"[A-Za-z0-9_-]{1,128}")
        _token(self.task_id, "task identity", r"[0-9a-f]{64}")
        _attempt_identity(self.attempt_id)
        _positive_fence(self.fence)
        if self.status not in {
            "SUBMISSION_PENDING",
            "SUBMITTED",
            "UNCERTAIN",
            "NOT_SUBMITTED",
            "SUCCEEDED",
            "FAILED",
            "CANCELLED",
        }:
            raise RuntimeError("Unknown recorded attempt state")
        if self.job_id is not None:
            _token(self.job_id, "service job identity", r"[A-Za-z0-9_-]{1,128}")
        elif self.status not in {"SUBMISSION_PENDING", "UNCERTAIN", "NOT_SUBMITTED"}:
            raise RuntimeError(
                "Recorded attempt state requires its service job identity"
            )


@dataclass(frozen=True)
class RecoveryDecision:
    action: Literal["WAIT", "RECONCILE", "RETRY_ALLOWED", "MANUAL_RECONCILIATION"]
    reason_code: str


def decide_recovery(
    attempt: RecordedAttempt,
    *,
    observed_job_status: str | None = None,
    result: VerifiedTaskResult | None = None,
    outputs_verified: bool = False,
    publication_checked: bool = False,
) -> RecoveryDecision:
    """Classify recovery only; never submit or infer that an expired writer stopped."""
    if type(outputs_verified) is not bool or type(publication_checked) is not bool:
        raise RuntimeError("Recovery requires explicit Boolean evidence flags")
    if result is not None:
        result = load_task_result(result.result_json)
    if observed_job_status not in {
        None,
        "NotStarted",
        "InProgress",
        "Completed",
        "Failed",
        "Cancelled",
    }:
        raise RuntimeError("Unknown service job state requires explicit reconciliation")
    if result is not None and (
        result.run_id,
        result.task_id,
        result.attempt_id,
        result.fence,
        result.job_id,
    ) != (
        attempt.run_id,
        attempt.task_id,
        attempt.attempt_id,
        attempt.fence,
        attempt.job_id,
    ):
        raise RuntimeError(
            "Recovered result identity or fence differs from its attempt"
        )
    if observed_job_status is not None and attempt.job_id is None:
        raise RuntimeError("Observed service state requires the recorded job identity")
    if observed_job_status in {"NotStarted", "InProgress"}:
        return RecoveryDecision("WAIT", "SERVICE_JOB_ACTIVE")
    if attempt.status == "NOT_SUBMITTED" and attempt.job_id is None:
        return RecoveryDecision("RETRY_ALLOWED", "DEFINITE_SUBMISSION_REJECTION")
    if attempt.job_id is None or observed_job_status is None:
        return RecoveryDecision(
            "MANUAL_RECONCILIATION", "SUBMISSION_OR_JOB_STATE_UNCERTAIN"
        )
    if result is not None:
        if result.status == "SUCCEEDED":
            if observed_job_status != "Completed":
                raise RuntimeError("A failed service job cannot assert task success")
            if not outputs_verified:
                return RecoveryDecision(
                    "MANUAL_RECONCILIATION", "OUTPUT_EVIDENCE_UNVERIFIED"
                )
        return RecoveryDecision("RECONCILE", "MATCHING_TERMINAL_RESULT")
    if observed_job_status in {"Failed", "Cancelled"} and publication_checked:
        return RecoveryDecision("RETRY_ALLOWED", "TERMINAL_JOB_AND_PUBLICATION_CHECKED")
    return RecoveryDecision(
        "MANUAL_RECONCILIATION", "RESULT_OR_PUBLICATION_EVIDENCE_MISSING"
    )


@dataclass(frozen=True)
class FileObservation:
    relative_path: str
    size_bytes: int
    modified_utc: str


@dataclass(frozen=True)
class SealedFileArrival:
    files: tuple[FileObservation, ...]
    observed_utc: str
    policy_json: str
    evidence_json: str

    @property
    def membership_sha256(self) -> str:
        return hashlib.sha256(
            _canonical(
                [
                    {
                        "relative_path": file.relative_path,
                        "size_bytes": file.size_bytes,
                        "modified_utc": file.modified_utc,
                    }
                    for file in self.files
                ]
            ).encode("ascii")
        ).hexdigest()


def _observation_time(value: object) -> datetime:
    text = _token(
        value,
        "UTC file observation instant",
        r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?Z",
    )
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        raise RuntimeError("Invalid UTC file observation instant") from None


def _file_observations(values: object) -> tuple[FileObservation, ...]:
    if not isinstance(values, (list, tuple)) or not 1 <= len(values) <= 5000:
        raise RuntimeError(
            "File membership must contain between 1 and 5000 explicit files"
        )
    files = []
    for value in values:
        if not isinstance(value, dict) or set(value) != {
            "relative_path",
            "size_bytes",
            "modified_utc",
        }:
            raise RuntimeError("Invalid file observation fields")
        path = _token(
            value["relative_path"],
            "relative source file",
            r"[A-Za-z0-9_][A-Za-z0-9_./ -]{0,1999}",
        )
        if any(part in {"", ".", ".."} for part in path.split("/")):
            raise RuntimeError("Source file membership cannot escape its root")
        size = value["size_bytes"]
        if type(size) is not int or not 0 <= size <= 2**63 - 1:
            raise RuntimeError("File size must be an explicit nonnegative bigint")
        modified = (
            _observation_time(value["modified_utc"])
            .isoformat(timespec="microseconds")
            .replace("+00:00", "Z")
        )
        files.append(FileObservation(path, size, modified))
    if len({file.relative_path for file in files}) != len(files):
        raise RuntimeError("Duplicate source file membership")
    return tuple(sorted(files, key=lambda file: file.relative_path))


def seal_file_arrival(
    files: Sequence[dict[str, object]],
    policy: dict[str, object],
    *,
    observed_utc: str,
    previous_files: Sequence[dict[str, object]] | None = None,
    previous_observed_utc: str | None = None,
    present_markers: Sequence[str] = (),
    allow_empty_files: bool = False,
) -> SealedFileArrival:
    """Seal reviewed remote observations; existence alone never establishes arrival."""
    if type(allow_empty_files) is not bool:
        raise RuntimeError("Empty-file acceptance must be explicitly Boolean")
    current = _file_observations(files)
    observed = _observation_time(observed_utc)
    if any(_observation_time(file.modified_utc) > observed for file in current):
        raise RuntimeError("Source modification time is later than its observation")
    if not allow_empty_files and any(file.size_bytes == 0 for file in current):
        raise RuntimeError("An empty file requires explicit acceptance")
    if not isinstance(policy, dict):
        raise TypeError("An explicit reviewed arrival policy is required")
    if policy.get("kind") == "marker":
        if set(policy) != {"kind", "marker"}:
            raise RuntimeError("Unknown completion-marker policy field")
        marker = _token(
            policy["marker"], "completion marker", r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,255}"
        )
        if not isinstance(present_markers, (list, tuple)) or any(
            not isinstance(value, str) for value in present_markers
        ):
            raise RuntimeError("Completion markers require an exact observed name list")
        if marker not in present_markers:
            raise RuntimeError("The configured completion marker is missing")
    elif policy.get("kind") == "stable":
        if set(policy) != {"kind", "minimum_age_seconds", "stable_for_seconds"}:
            raise RuntimeError("Unknown stable-arrival policy field")
        age, interval = policy["minimum_age_seconds"], policy["stable_for_seconds"]
        if any(type(n) is not int or not 1 <= n <= 604800 for n in (age, interval)):
            raise RuntimeError(
                "Stable arrival requires explicit bounded positive intervals"
            )
        if previous_files is None or previous_observed_utc is None:
            raise RuntimeError("Stable arrival requires two explicit observations")
        previous = _observation_time(previous_observed_utc)
        if any(_observation_time(file.modified_utc) > previous for file in current):
            raise RuntimeError(
                "A previous observation cannot precede a file modification"
            )
        if (observed - previous).total_seconds() < interval or _file_observations(
            previous_files
        ) != current:
            raise RuntimeError("Source file membership has not remained stable")
        if any(
            (observed - _observation_time(file.modified_utc)).total_seconds() < age
            for file in current
        ):
            raise RuntimeError(
                "A source file is too recent for the reviewed arrival policy"
            )
    else:
        raise RuntimeError("Unsupported arrival policy; file existence is insufficient")
    return SealedFileArrival(
        current,
        observed.isoformat(timespec="microseconds").replace("+00:00", "Z"),
        _canonical(policy),
        _canonical(
            {
                "previous_files": [
                    {
                        "relative_path": file.relative_path,
                        "size_bytes": file.size_bytes,
                        "modified_utc": file.modified_utc,
                    }
                    for file in _file_observations(previous_files)
                ]
                if policy["kind"] == "stable"
                else None,
                "previous_observed_utc": previous_observed_utc
                if policy["kind"] == "stable"
                else None,
                "present_markers": list(present_markers)
                if policy["kind"] == "marker"
                else [],
                "allow_empty_files": allow_empty_files,
            }
        ),
    )


def _file_arrival_manifest(
    manifest: VerifiedRunManifest, document: object
) -> dict[str, SealedFileArrival]:
    if not isinstance(document, dict) or set(document) != {
        "format_version",
        "run_id",
        "version_id",
        "snapshot_sha256",
        "arrivals",
    }:
        raise RuntimeError("Invalid persisted file-arrival fields")
    if (
        type(document["format_version"]) is not int
        or document["format_version"] != 1
        or document["run_id"] != manifest.run_id
        or document["version_id"] != manifest.version_id
        or document["snapshot_sha256"] != manifest.snapshot_sha256
    ):
        raise RuntimeError("File arrivals differ from their pinned run")
    payload = manifest.to_document()
    configuration = cast(dict[str, Any], payload["configuration"])
    feeds = {feed["id"]: feed for feed in configuration["feeds"]}
    objects = {obj["id"]: obj for obj in configuration["objects"]}
    tasks = {
        task["object_id"]: task
        for task in cast(list[dict[str, Any]], payload["tasks"])
        if task["layer"] == "bronze"
        and feeds[task["feed_id"]]["source_type"] in ("ftp", "sftp")
    }
    rows = document["arrivals"]
    if not isinstance(rows, list) or len(rows) != len(tasks):
        raise RuntimeError("File arrivals omit selected object membership")
    result = {}
    for row in rows:
        if not isinstance(row, dict) or set(row) != {
            "object_id",
            "source_batch_id",
            "observed_utc",
            "policy",
            "files",
            "evidence",
        }:
            raise RuntimeError("Invalid persisted file-arrival object")
        identity = row["object_id"]
        if not isinstance(identity, str) or identity not in tasks or identity in result:
            raise RuntimeError("Duplicate or unselected file-arrival object")
        obj, task = objects[identity], tasks[identity]
        if (
            row["source_batch_id"] != task["source_batch_id"]
            or _canonical(row["policy"])
            != _canonical(obj["contract"].get("arrival_policy"))
            or _observation_time(row["observed_utc"])
            < _observation_time(payload["observed_utc"])
        ):
            raise RuntimeError(
                "File arrival policy, batch or observation differs from its pinned source"
            )
        evidence = row["evidence"]
        if not isinstance(evidence, dict) or set(evidence) != {
            "previous_files",
            "previous_observed_utc",
            "present_markers",
            "allow_empty_files",
        }:
            raise RuntimeError(
                "File arrival lacks its retained marker or stability evidence"
            )
        sealed = seal_file_arrival(
            row["files"], row["policy"], observed_utc=row["observed_utc"], **evidence
        )
        pattern = obj["contract"].get("file_pattern")
        recursive = obj["contract"].get("recursive")
        if (
            not isinstance(pattern, str)
            or type(recursive) is not bool
            or any(
                not fnmatch.fnmatchcase(PurePosixPath(file.relative_path).name, pattern)
                or (not recursive and "/" in file.relative_path)
                or file.relative_path == row["policy"].get("marker")
                for file in sealed.files
            )
        ):
            raise RuntimeError(
                "Observed file membership differs from its pinned selection pattern"
            )
        result[identity] = sealed
    return result


def write_file_arrival_manifest(
    fs: FileSystem,
    manifest: VerifiedRunManifest,
    arrivals: Mapping[str, SealedFileArrival],
) -> dict[str, str]:
    tasks = {
        task["object_id"]: task
        for task in cast(list[dict[str, Any]], manifest.to_document()["tasks"])
        if task["layer"] == "bronze"
    }
    rows = []
    for identity, arrival in sorted(arrivals.items()):
        if identity not in tasks or not isinstance(arrival, SealedFileArrival):
            raise RuntimeError("File arrival belongs to an unselected object")
        rows.append(
            {
                "object_id": identity,
                "source_batch_id": tasks[identity]["source_batch_id"],
                "observed_utc": arrival.observed_utc,
                "policy": json.loads(arrival.policy_json),
                "evidence": json.loads(arrival.evidence_json),
                "files": [
                    {
                        "relative_path": file.relative_path,
                        "size_bytes": file.size_bytes,
                        "modified_utc": file.modified_utc,
                    }
                    for file in arrival.files
                ],
            }
        )
    document = {
        "format_version": 1,
        "run_id": manifest.run_id,
        "version_id": manifest.version_id,
        "snapshot_sha256": manifest.snapshot_sha256,
        "arrivals": rows,
    }
    _file_arrival_manifest(manifest, document)
    text = _canonical(document)
    digest = hashlib.sha256(text.encode("ascii")).hexdigest()
    path = _manifest_path(
        f"Files/orchestration/{manifest.run_id}/file_arrivals.json", manifest.run_id
    )
    _write_immutable(fs, path, text, MAX_MANIFEST_BYTES)
    return {"path": path, "sha256": digest}


def read_file_arrival_manifest(
    fs: FileSystem, path: str, manifest: VerifiedRunManifest, *, expected_sha256: str
) -> dict[str, SealedFileArrival]:
    text = _read_bounded(fs, _manifest_path(path, manifest.run_id), MAX_MANIFEST_BYTES)
    if hashlib.sha256(text.encode("ascii")).hexdigest() != expected_sha256:
        raise RuntimeError("File-arrival manifest differs from its immutable hash")
    try:
        document = json.loads(text)
    except ValueError:
        raise RuntimeError("File-arrival manifest is not finite JSON") from None
    return _file_arrival_manifest(manifest, document)


def verify_file_membership(
    sealed: SealedFileArrival, files: Sequence[dict[str, object]]
) -> None:
    """Reject changed source membership when reobserved around a transport operation."""
    if _file_observations(files) != sealed.files:
        raise RuntimeError("Sealed source file membership changed during landing")


@dataclass(frozen=True)
class VerifiedBronzeReceipt:
    run_id: str
    feed_id: str
    receipt_json: str

    @property
    def receipt_sha256(self) -> str:
        return hashlib.sha256(self.receipt_json.encode("ascii")).hexdigest()

    def to_document(self) -> dict[str, object]:
        return cast(dict[str, object], json.loads(self.receipt_json))


def _bronze_path(value: object, prefix: str) -> str:
    path = _token(
        value, "Bronze membership path", r"Files/bronze/[A-Za-z0-9_./ -]{1,1950}"
    )
    if not path.startswith(prefix + "/") or any(
        part in {"", ".", ".."} for part in path.split("/")
    ):
        raise RuntimeError("Bronze file membership escapes its pinned batch scope")
    return path


def _file_size(fs: FileSystem, path: str) -> int:
    if not fs.exists(path):
        raise FileNotFoundError("A selected Bronze file is missing")
    location = PurePosixPath(path)
    entries = [
        entry for entry in fs.ls(str(location.parent)) if entry.name == location.name
    ]
    if len(entries) != 1 or getattr(entries[0], "isDir", False):
        raise RuntimeError("Bronze file membership is missing or ambiguous")
    size = entries[0].size
    if type(size) is not int or not 0 <= size <= 2**63 - 1:
        raise RuntimeError("Bronze file size is not a valid nonnegative bigint")
    return size


def _verify_landed_membership(
    fs: FileSystem, target: str, expected: Mapping[str, int]
) -> None:
    if not fs.exists(target):
        raise FileNotFoundError("The pinned Bronze object folder is missing")
    pending = [target]
    seen = set()
    actual = {}
    while pending:
        directory = pending.pop()
        entries = fs.ls(directory)
        if len(entries) + len(seen) > 10000:
            raise RuntimeError("Landed Bronze membership exceeds its bounded inventory")
        for entry in entries:
            name = _token(
                entry.name, "landed file name", r"[A-Za-z0-9_][A-Za-z0-9_. -]{0,1999}"
            )
            path = _bronze_path(directory + "/" + name, target)
            if path in seen:
                raise RuntimeError("Duplicate landed Bronze membership")
            seen.add(path)
            is_directory = getattr(entry, "isDir", False)
            if type(is_directory) is not bool:
                raise RuntimeError("Invalid landed Bronze directory metadata")
            if is_directory:
                pending.append(path)
            else:
                size = entry.size
                if type(size) is not int or not 0 <= size <= 2**63 - 1:
                    raise RuntimeError("Invalid landed Bronze file size")
                actual[path] = size
    if actual != dict(expected):
        raise RuntimeError("Landed Bronze membership differs from its sealed selection")


def _verify_context_file_membership(fs: FileSystem, context: Mapping[str, Any]) -> None:
    if context["source_type"] == "sql":
        return
    for name, target in context["raw_targets"].items():
        _verify_landed_membership(
            fs,
            target,
            dict(
                zip(
                    context["raw_source_files"][name],
                    context["raw_source_sizes"][name],
                    strict=True,
                )
            ),
        )


def _feed_membership(manifest: VerifiedRunManifest, feed_id: str):
    document = manifest.to_document()
    configuration = cast(dict[str, object], document["configuration"])
    feeds = cast(list[dict[str, object]], configuration["feeds"])
    matches = [feed for feed in feeds if feed["id"] == feed_id]
    if len(matches) != 1:
        raise RuntimeError("Bronze receipt feed is missing or ambiguous")
    tasks = [
        task
        for task in cast(list[dict[str, object]], document["tasks"])
        if task["feed_id"] == feed_id and task["layer"] == "bronze"
    ]
    if not tasks or len({task["source_batch_id"] for task in tasks}) != 1:
        raise RuntimeError("Bronze receipt requires one complete pinned source batch")
    objects = {
        obj["id"]: obj
        for obj in cast(list[dict[str, object]], configuration["objects"])
    }
    return document, matches[0], tasks, objects


def _sealed_bronze_table(
    fs: FileSystem,
    feed: dict[str, Any],
    task: dict[str, Any],
    obj: dict[str, Any],
    file_arrivals: Mapping[str, SealedFileArrival] | None,
) -> dict[str, Any]:
    target = task["target_path"]
    selection_hash = None
    if feed["source_type"] == "sql":
        files = [{"path": target, "size_bytes": _file_size(fs, target)}]
        if files[0]["size_bytes"] == 0:
            raise RuntimeError("A SQL CSV extraction must contain its header")
    else:
        arrival = file_arrivals.get(obj["id"]) if file_arrivals is not None else None
        if not isinstance(arrival, SealedFileArrival):
            raise RuntimeError("File feeds require a sealed reviewed arrival selection")
        policy = obj["contract"].get("arrival_policy")
        if not isinstance(policy, dict) or arrival.policy_json != _canonical(policy):
            raise RuntimeError(
                "File arrival policy differs from its pinned object metadata"
            )
        selection_hash = arrival.membership_sha256
        files = [
            {
                "path": _bronze_path(target + "/" + member.relative_path, target),
                "size_bytes": member.size_bytes,
            }
            for member in arrival.files
        ]
        _verify_landed_membership(
            fs, target, {file["path"]: file["size_bytes"] for file in files}
        )
    return {
        "object_id": obj["id"],
        "table_name": obj["name"],
        "target_path": target,
        "files": files,
        "source_selection_sha256": selection_hash,
    }


def seal_bronze_receipt(
    fs: FileSystem,
    manifest: VerifiedRunManifest,
    feed_id: str,
    *,
    file_arrivals: Mapping[str, SealedFileArrival] | None = None,
) -> VerifiedBronzeReceipt:
    """Persistable file membership; Spark inventory seals the business record multiset."""
    document, feed, tasks, objects = _feed_membership(manifest, feed_id)
    tables = []
    for task in tasks:
        obj = objects[task["object_id"]]
        tables.append(_sealed_bronze_table(fs, feed, task, obj, file_arrivals))
    receipt = {
        "format_version": 1,
        "run_id": manifest.run_id,
        "version_id": manifest.version_id,
        "definition_sha256": document["definition_sha256"],
        "feed_id": feed_id,
        "source_batch_id": tasks[0]["source_batch_id"],
        "observed_utc": document["observed_utc"],
        "tables": [
            {key: value for key, value in table.items() if key != "runtime_layout"}
            for table in tables
        ],
    }
    result = VerifiedBronzeReceipt(manifest.run_id, feed_id, _canonical(receipt))
    _validate_bronze_receipt(result, manifest, feed_id)
    return result


def _validate_bronze_receipt(
    receipt: VerifiedBronzeReceipt, manifest: VerifiedRunManifest, feed_id: str
) -> None:
    document, feed, tasks, objects = _feed_membership(manifest, feed_id)
    value = receipt.to_document()
    expected = {
        "format_version",
        "run_id",
        "version_id",
        "definition_sha256",
        "feed_id",
        "source_batch_id",
        "observed_utc",
        "tables",
    }
    if (
        set(value) != expected
        or type(value["format_version"]) is not int
        or value["format_version"] != 1
    ):
        raise RuntimeError("Unsupported Bronze receipt contract")
    for name, expected_value in (
        ("run_id", manifest.run_id),
        ("version_id", manifest.version_id),
        ("definition_sha256", document["definition_sha256"]),
        ("feed_id", feed_id),
        ("source_batch_id", tasks[0]["source_batch_id"]),
        ("observed_utc", document["observed_utc"]),
    ):
        if value[name] != expected_value:
            raise RuntimeError(
                "Bronze receipt identity differs from its pinned manifest"
            )
    tables = value["tables"]
    if not isinstance(tables, list) or len(tables) != len(tasks):
        raise RuntimeError("Bronze receipt omits selected task membership")
    by_id = {task["object_id"]: task for task in tasks}
    seen = set()
    all_files = set()
    for table in tables:
        if not isinstance(table, dict) or set(table) != {
            "object_id",
            "table_name",
            "target_path",
            "files",
            "source_selection_sha256",
        }:
            raise RuntimeError("Invalid Bronze receipt table fields")
        identity = table["object_id"]
        if not isinstance(identity, str) or identity not in by_id or identity in seen:
            raise RuntimeError("Duplicate or unselected Bronze receipt object")
        seen.add(identity)
        task = by_id[identity]
        if (
            table["target_path"] != task["target_path"]
            or table["table_name"] != objects[identity]["name"]
        ):
            raise RuntimeError("Bronze receipt target differs from the pinned object")
        files = table["files"]
        if not isinstance(files, list) or not 1 <= len(files) <= 5000:
            raise RuntimeError(
                "Bronze receipt requires bounded explicit file membership"
            )
        target = cast(str, task["target_path"])
        if feed["source_type"] == "sql" and (
            len(files) != 1 or table["source_selection_sha256"] is not None
        ):
            raise RuntimeError(
                "SQL extraction membership must contain one contracted CSV"
            )
        if feed["source_type"] != "sql":
            _token(
                table["source_selection_sha256"], "sealed arrival hash", r"[0-9a-f]{64}"
            )
        for file in files:
            if not isinstance(file, dict) or set(file) != {"path", "size_bytes"}:
                raise RuntimeError("Invalid Bronze file-membership fields")
            path = file["path"]
            if feed["source_type"] == "sql":
                if path != target:
                    raise RuntimeError(
                        "SQL receipt file differs from its pinned target"
                    )
            else:
                _bronze_path(path, target)
            size = file["size_bytes"]
            if (
                type(size) is not int
                or not 0 <= size <= 2**63 - 1
                or (feed["source_type"] == "sql" and size == 0)
            ):
                raise RuntimeError("Invalid Bronze receipt file size")
            if path in all_files:
                raise RuntimeError("A Bronze file cannot belong to multiple objects")
            all_files.add(path)


def write_bronze_receipt(fs: FileSystem, receipt: VerifiedBronzeReceipt) -> str:
    feed_key = hashlib.sha256(receipt.feed_id.encode("utf-8")).hexdigest()
    path = _manifest_path(
        f"Files/orchestration/{receipt.run_id}/bronze/{feed_key}.json", receipt.run_id
    )
    return _write_immutable(fs, path, receipt.receipt_json, MAX_MANIFEST_BYTES)


def read_bronze_receipt(
    fs: FileSystem,
    path: str,
    manifest: VerifiedRunManifest,
    *,
    expected_sha256: str,
    feed_id: str,
) -> VerifiedBronzeReceipt:
    text = _read_bounded(fs, _manifest_path(path, manifest.run_id), MAX_MANIFEST_BYTES)
    try:
        value = json.loads(text)
        canonical = _canonical(value)
    except (ValueError, TypeError):
        raise RuntimeError("Bronze receipt is not finite JSON") from None
    if hashlib.sha256(canonical.encode("ascii")).hexdigest() != expected_sha256:
        raise RuntimeError("Bronze receipt hash differs from its immutable reference")
    result = VerifiedBronzeReceipt(manifest.run_id, feed_id, canonical)
    _validate_bronze_receipt(result, manifest, feed_id)
    return result


def source_runtime_config(
    manifest: VerifiedRunManifest,
    receipt: VerifiedBronzeReceipt,
    *,
    load_configuration: Callable[[object], PinnedConfiguration],
) -> dict[str, object]:
    """Reconstruct source contracts only from pinned metadata and sealed file membership."""
    _validate_bronze_receipt(receipt, manifest, receipt.feed_id)
    document, feed, _tasks, objects = _feed_membership(manifest, receipt.feed_id)
    configuration = cast(dict[str, object], document["configuration"])
    verified = load_configuration(configuration)
    if (
        verified.definition_sha256 != document["definition_sha256"]
        or verified.version_id != manifest.version_id
    ):
        raise RuntimeError(
            "Source runtime metadata does not match its pinned definition"
        )
    selected = {
        task["object_id"] for task in cast(list[dict[str, object]], document["tasks"])
    }
    silver = [
        obj
        for obj in objects.values()
        if obj["feed_id"] == receipt.feed_id
        and obj["layer"] == "silver"
        and obj["enabled"]
    ]
    if not silver:
        raise RuntimeError("Source runtime requires an explicit Silver object contract")
    lakehouses = {obj["lakehouse"] for obj in silver}
    if len(lakehouses) != 1 or any(
        obj["target_schema"] != feed["silver_schema"] for obj in silver
    ):
        raise RuntimeError(
            "Source Silver objects disagree on Lakehouse or schema ownership"
        )
    policies = cast(
        dict[str, object],
        cast(dict[str, object], configuration["extensions"])["hash_policies"],
    )
    tables = []
    selected_tables = []
    for obj in sorted(
        silver,
        key=lambda item: (cast(int, item["execution_priority"]), str(item["id"])),
    ):
        contract = json.loads(_canonical(obj["contract"]))
        contract["name"] = obj["name"]
        contract["keys"] = obj["business_keys"]
        contract["columns"] = obj["columns"]
        contract["foreign_keys"] = contract.get("foreign_keys", [])
        contract["allow_empty"] = contract.get("allow_empty", False)
        contract["change_hash_exclusions"] = contract.get("change_hash_exclusions", [])
        if obj["id"] not in policies:
            raise RuntimeError("Source runtime requires an explicit stable hash policy")
        contract["hash_policy"] = policies[obj["id"]]
        contract["rule_specs"] = [
            {name: value for name, value in rule.items() if name != "object_id"}
            for rule in cast(list[dict[str, object]], configuration["rules"])
            if rule["object_id"] == obj["id"]
            and rule["handler"] != "legacy.source_business_checks"
        ]
        contract["runtime_layout"] = {
            "layout": obj.get("layout"),
            "capabilities": cast(dict[str, object], configuration["extensions"]).get(
                "layout_runtime"
            ),
        }
        tables.append(contract)
        if obj["id"] in selected:
            selected_tables.append(obj["name"])
    if not selected_tables:
        raise RuntimeError("No Silver work was selected for this source runtime")
    membership = receipt.to_document()
    files = {}
    sizes = {}
    targets = {}
    for table in cast(list[dict[str, object]], membership["tables"]):
        name = cast(str, table["table_name"])
        files[name] = [
            file["path"] for file in cast(list[dict[str, object]], table["files"])
        ]
        sizes[name] = [
            file["size_bytes"] for file in cast(list[dict[str, object]], table["files"])
        ]
        targets[name] = table["target_path"]
    source_type = str(feed["source_type"])
    bronze_root = (
        f"Files/bronze/{feed['source_code']}_{feed['description_slug']}_{source_type}"
    )
    business_contract_hash = feed.get("legacy_contract_hash")
    if business_contract_hash is None:
        business_contract_hash = hashlib.sha256(
            _canonical(
                {
                    "source_code": feed["source_code"],
                    "silver_schema": feed["silver_schema"],
                    "tables": tables,
                }
            ).encode("ascii")
        ).hexdigest()
    return {
        "source_code": feed["source_code"],
        "lakehouse": next(iter(lakehouses)),
        "silver_schema": feed["silver_schema"],
        "quarantine_schema": feed["quarantine_schema"],
        "bronze_root": bronze_root,
        "contract_hash": business_contract_hash,
        "seed_snapshot_date": str(document["observed_utc"])[:10],
        "tables": tables,
        "metadata_version": manifest.version_id,
        "metadata_run_id": manifest.run_id,
        "source_feed_id": receipt.feed_id,
        "source_batch_id": membership["source_batch_id"],
        "source_type": source_type,
        "description_slug": feed["description_slug"],
        "observed_utc": document["observed_utc"],
        "processing_date": str(document["observed_utc"])[:10],
        "pipeline_run_id": membership["source_batch_id"],
        "raw_source_files": files,
        "raw_source_sizes": sizes,
        "raw_targets": targets,
        "source_selection_complete": len(selected_tables) == len(silver),
        "required_source_tables": sorted(str(obj["name"]) for obj in silver),
        "selected_tables": sorted(cast(list[str], selected_tables)),
        "bronze_receipt_sha256": receipt.receipt_sha256,
        "source_rule_specs": [
            {name: value for name, value in rule.items() if name != "object_id"}
            for rule in cast(list[dict[str, object]], configuration["rules"])
            if rule["object_id"] in {obj["id"] for obj in silver}
            and rule["handler"] == "legacy.source_business_checks"
        ],
    }


@dataclass(frozen=True)
class VerifiedPipelineDispatch:
    run_id: str
    layer: str
    dispatch_json: str

    @property
    def dispatch_sha256(self) -> str:
        return hashlib.sha256(self.dispatch_json.encode("ascii")).hexdigest()

    def to_document(self) -> dict[str, object]:
        return cast(dict[str, object], json.loads(self.dispatch_json))


def _pipeline_activity_scope(location: object) -> tuple[str, str]:
    value = _token(
        location,
        "pipeline activity scope",
        r"https://api\.fabric\.microsoft\.com/v1/workspaces/[0-9a-f-]{36}/"
        r"datapipelines/pipelineruns/[0-9a-f-]{36}/queryactivityruns",
    )
    parts = value.split("/")
    workspace, execution = parts[5], parts[8]
    if str(UUID(workspace)) != workspace or str(UUID(execution)) != execution:
        raise RuntimeError("Pipeline activity scope requires canonical resolved UUIDs")
    return workspace, execution


def make_pipeline_dispatch(
    manifest: VerifiedRunManifest, document: object
) -> VerifiedPipelineDispatch:
    fields = {
        "format_version",
        "run_id",
        "version_id",
        "snapshot_sha256",
        "layer",
        "attempts",
    }
    if not isinstance(document, dict) or not document:
        raise RuntimeError("Invalid authoritative pipeline dispatch fields")
    partial = document.get("format_version") == 2
    if partial or (
        document.get("format_version") == 1
        and document.get("layer") in ("silver", "gold")
        and "undispatched_task_ids" in document
        and document["undispatched_task_ids"] is None
    ):
        fields.add("undispatched_task_ids")
    if set(document) != fields:
        raise RuntimeError("Invalid authoritative pipeline dispatch fields")
    if (
        type(document["format_version"]) is not int
        or document["format_version"] not in (1, 2)
        or (partial and document["layer"] != "bronze")
        or document["run_id"] != manifest.run_id
        or document["version_id"] != manifest.version_id
        or document["snapshot_sha256"] != manifest.snapshot_sha256
        or document["layer"] not in {"bronze", "silver", "gold"}
    ):
        raise RuntimeError("Pipeline dispatch differs from its pinned run")
    tasks = {
        task["task_id"]: task
        for task in cast(list[dict[str, object]], manifest.to_document()["tasks"])
        if task["layer"] == document["layer"]
    }
    attempts = document["attempts"]
    missing = document["undispatched_task_ids"] if partial else []
    if (
        not tasks
        or not isinstance(attempts, list)
        or not isinstance(missing, list)
        or any(not isinstance(identity, str) for identity in missing)
        or len(set(missing)) != len(missing)
        or not set(missing) <= tasks.keys()
        or len(attempts) + len(missing) != len(tasks)
    ):
        raise RuntimeError("Pipeline dispatch omits selected layer membership")
    identities, seen = set(), set()
    for attempt in attempts:
        if not isinstance(attempt, dict) or set(attempt) != {
            "task_id",
            "attempt_id",
            "fence",
            "source_fence",
            "status",
            "job_location",
        }:
            raise RuntimeError("Invalid pipeline prepared-attempt fields")
        task_id = _token(attempt["task_id"], "pipeline task identity", r"[0-9a-f]{64}")
        identity = _attempt_identity(attempt["attempt_id"])
        if (
            task_id not in tasks
            or task_id in missing
            or task_id in seen
            or identity in identities
        ):
            raise RuntimeError("Duplicate or unselected pipeline attempt")
        _positive_fence(attempt["fence"])
        if tasks[task_id]["source_lock_key"] is not None:
            _positive_fence(attempt["source_fence"])
        elif attempt["source_fence"] is not None:
            raise RuntimeError("A feed-independent task cannot invent a source fence")
        if attempt["status"] not in {
            "SUBMISSION_PENDING",
            "SUBMITTED",
            "UNCERTAIN",
            "SUCCEEDED",
            "FAILED",
            "CANCELLED",
        }:
            raise RuntimeError("Invalid pipeline prepared-attempt state")
        _pipeline_activity_scope(attempt["job_location"])
        seen.add(task_id)
        identities.add(identity)
    return VerifiedPipelineDispatch(
        manifest.run_id, document["layer"], _canonical(document)
    )


def read_pipeline_dispatch(
    fs: FileSystem,
    path: str,
    manifest: VerifiedRunManifest,
    *,
    expected_sha256: str,
    layer: str,
) -> VerifiedPipelineDispatch:
    text = _read_bounded(fs, _manifest_path(path, manifest.run_id), MAX_MANIFEST_BYTES)
    if path.endswith(".csv"):
        reader = csv.DictReader(io.StringIO(text), escapechar="\\")
        if reader.fieldnames != ["dispatch_json"]:
            raise RuntimeError("Pipeline dispatch export columns differ")
        rows = list(reader)
        if len(rows) != 1 or None in rows[0] or rows[0]["dispatch_json"] is None:
            raise RuntimeError("Pipeline dispatch export must contain one complete row")
        text = rows[0]["dispatch_json"]
    try:
        document = json.loads(text)
    except ValueError:
        raise RuntimeError("Pipeline dispatch is not finite JSON") from None
    dispatch = make_pipeline_dispatch(manifest, document)
    if dispatch.layer != layer or dispatch.dispatch_sha256 != expected_sha256:
        raise RuntimeError(
            "Pipeline dispatch hash or layer differs from SQL preparation"
        )
    return dispatch


def pipeline_activity_evidence(
    client: ActivityClient,
    manifest: VerifiedRunManifest,
    dispatch: VerifiedPipelineDispatch,
    *,
    observed_utc: str,
    parent_activity: str | None = None,
    allow_unresolved: bool = False,
) -> dict[str, dict[str, str]]:
    if type(allow_unresolved) is not bool or (
        allow_unresolved and dispatch.layer != "bronze"
    ):
        raise RuntimeError(
            "Only Bronze recovery may retain unresolved activity evidence"
        )
    checked = make_pipeline_dispatch(manifest, dispatch.to_document())
    if (
        checked.dispatch_sha256 != dispatch.dispatch_sha256
        or (dispatch.layer == "bronze" and parent_activity is not None)
        or (
            dispatch.layer != "bronze"
            and parent_activity != f"orchestrate_{dispatch.layer}"
        )
    ):
        raise RuntimeError(
            "Activity reconciliation requires its authoritative dispatch and operation"
        )
    now = _observation_time(observed_utc)
    observed = _observation_time(manifest.to_document()["observed_utc"])
    if now < observed:
        raise RuntimeError("Activity observation cannot precede its pinned run")
    attempts = cast(list[dict[str, Any]], dispatch.to_document()["attempts"])
    by_scope: dict[str, list[dict[str, Any]]] = {}
    for attempt in attempts:
        by_scope.setdefault(attempt["job_location"], []).append(attempt)
    result = {}
    for location, selected in by_scope.items():
        _workspace, execution = _pipeline_activity_scope(location)
        response = client.request(
            "POST",
            location,
            body={
                "filters": [],
                "orderBy": [{"orderBy": "ActivityRunStart", "order": "ASC"}],
                "lastUpdatedAfter": observed.isoformat(timespec="microseconds").replace(
                    "+00:00", "Z"
                ),
                "lastUpdatedBefore": now.isoformat(timespec="microseconds").replace(
                    "+00:00", "Z"
                ),
            },
            expected=(200,),
        )
        if (
            not isinstance(response.content, bytes)
            or not 1 <= len(response.content) <= MAX_MANIFEST_BYTES
        ):
            raise RuntimeError(
                "Pipeline activity response exceeds its bounded contract"
            )
        try:
            activities = json.loads(response.content.decode("utf-8"))
        except (ValueError, UnicodeError):
            raise RuntimeError("Pipeline activity response is not valid JSON") from None
        if not isinstance(activities, list) or len(activities) > 50000:
            raise RuntimeError("Pipeline activity membership is invalid or truncated")
        for attempt in selected:
            matches = [
                activity
                for activity in activities
                if isinstance(activity, dict)
                and activity.get("pipelineRunId") == execution
                and activity.get("activityType")
                == ("Copy" if dispatch.layer == "bronze" else "TridentNotebook")
                and isinstance(activity.get("userProperties"), dict)
                and activity["userProperties"].get("run_id") == manifest.run_id
                and (
                    (
                        dispatch.layer == "bronze"
                        and activity["userProperties"].get("task_id")
                        == attempt["task_id"]
                        and activity["userProperties"].get("attempt_id")
                        == attempt["attempt_id"]
                    )
                    or (
                        dispatch.layer != "bronze"
                        and activity.get("activityName") == parent_activity
                        and activity["userProperties"].get("layer") == dispatch.layer
                        and activity["userProperties"].get("dispatch_sha256")
                        == dispatch.dispatch_sha256
                    )
                )
            ]
            if len(matches) != 1:
                if allow_unresolved:
                    result[attempt["task_id"]] = {
                        "observed_job_status": "Unresolved",
                        "error_code": "COPY_ACTIVITY_NOT_VISIBLE"
                        if not matches
                        else "COPY_ACTIVITY_AMBIGUOUS",
                    }
                    continue
                raise RuntimeError(
                    "Copy activity identity is missing or ambiguous; reconcile without resubmission"
                )
            activity = matches[0]
            if activity.get("retryAttempt") not in (None, 0):
                raise RuntimeError(
                    "A retried Copy cannot certify an immutable extraction"
                )
            states = {
                "Succeeded": "Completed",
                "Failed": "Failed",
                "Cancelled": "Cancelled",
            }
            if activity.get("status") not in states:
                if allow_unresolved:
                    result[attempt["task_id"]] = {
                        "observed_job_status": "Unresolved",
                        "error_code": "COPY_ACTIVITY_NOT_TERMINAL",
                    }
                    continue
                raise RuntimeError(
                    "Copy activity is active or uncertain; reconciliation must wait"
                )
            identity = _attempt_identity(activity.get("activityRunId"))
            end = _token(
                activity.get("activityRunEnd"),
                "terminal activity instant",
                r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,7})?Z",
            )
            try:
                finished = datetime.fromisoformat(end)
            except ValueError:
                raise RuntimeError("Invalid terminal activity instant") from None
            if finished < observed or finished > now:
                raise RuntimeError(
                    "Terminal activity time differs from its observation"
                )
            proof = {
                "job_id": identity,
                "job_location": location,
                "observed_job_status": states[activity["status"]],
                "finished_utc": finished.isoformat(timespec="microseconds").replace(
                    "+00:00", "Z"
                ),
                "run_id": manifest.run_id,
                "task_id": attempt["task_id"],
                "attempt_id": attempt["attempt_id"],
            }
            result[attempt["task_id"]] = {
                key: proof[key]
                for key in (
                    "job_id",
                    "job_location",
                    "observed_job_status",
                    "finished_utc",
                )
            } | {
                "job_evidence_sha256": hashlib.sha256(
                    _canonical(proof).encode("ascii")
                ).hexdigest()
            }
    return result


def write_pipeline_callbacks(
    fs: FileSystem,
    manifest: VerifiedRunManifest,
    results: Sequence[tuple[VerifiedTaskResult, Mapping[str, str]]],
    *,
    max_bytes: int = 4 * 1024 * 1024 - 1024,
) -> list[dict[str, object]]:
    if type(max_bytes) is not int or not 1 <= max_bytes < 4 * 1024 * 1024:
        raise RuntimeError("Invalid bounded SQL callback page size")
    columns = [
        "result_json",
        "result_sha256",
        "observed_job_status",
        "job_evidence_sha256",
        "job_location",
    ]
    rows = []
    seen = set()
    task_ids = {
        task["task_id"]
        for task in cast(list[dict[str, object]], manifest.to_document()["tasks"])
    }
    for result, evidence in results:
        result = load_task_result(result.result_json)
        if (
            result.run_id != manifest.run_id
            or result.task_id not in task_ids
            or (result.task_id, result.attempt_id) in seen
        ):
            raise RuntimeError("Duplicate or out-of-run SQL callback")
        seen.add((result.task_id, result.attempt_id))
        read_task_result(
            fs,
            _result_path(result),
            run_id=result.run_id,
            task_id=result.task_id,
            attempt_id=result.attempt_id,
            expected_sha256=result.result_sha256,
        )
        _pipeline_activity_scope(evidence["job_location"])
        if evidence["job_id"] != result.job_id or evidence[
            "observed_job_status"
        ] not in {"Completed", "Failed", "Cancelled"}:
            raise RuntimeError("SQL callback differs from its service evidence")
        if (
            result.status == "SUCCEEDED"
            and evidence["observed_job_status"] != "Completed"
        ):
            raise RuntimeError(
                "A successful callback requires successful service evidence"
            )
        _token(
            evidence["job_evidence_sha256"], "service evidence hash", r"[0-9a-f]{64}"
        )
        rows.append(
            {
                "result_json": result.result_json,
                "result_sha256": result.result_sha256,
                **{key: evidence[key] for key in columns[2:]},
            }
        )
    pages, current = [], []

    def flush() -> None:
        if not current:
            return
        stream = io.StringIO(newline="")
        writer = csv.DictWriter(
            stream, fieldnames=columns, escapechar="\\", quoting=csv.QUOTE_ALL
        )
        writer.writeheader()
        writer.writerows(current)
        text = stream.getvalue()
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
        path = _manifest_path(
            f"Files/orchestration/{manifest.run_id}/callbacks/{digest}.csv",
            manifest.run_id,
        )
        _write_immutable(fs, path, text, MAX_MANIFEST_BYTES)
        pages.append({"path": path, "sha256": digest, "rows": len(current)})
        current.clear()

    for row in rows:
        if (
            len(current) >= 5000
            or len(_canonical({"value": [*current, row]}).encode("ascii")) > max_bytes
        ):
            flush()
        if len(_canonical({"value": [row]}).encode("ascii")) > max_bytes:
            raise RuntimeError("A SQL callback exceeds its response bound")
        current.append(row)
    flush()
    return pages


def verify_pipeline_callbacks(
    fs: FileSystem,
    manifest: VerifiedRunManifest,
    path: str,
    expected_sha256: str,
) -> None:
    text = _read_bounded(
        fs, _manifest_path(path, manifest.run_id), 4 * 1024 * 1024 - 1024
    )
    if hashlib.sha256(text.encode("utf-8")).hexdigest() != expected_sha256:
        raise RuntimeError("SQL callback file differs from its immutable page hash")
    reader = csv.DictReader(io.StringIO(text), escapechar="\\")
    if reader.fieldnames != [
        "result_json",
        "result_sha256",
        "observed_job_status",
        "job_evidence_sha256",
        "job_location",
    ]:
        raise RuntimeError("SQL callback columns differ from their contract")
    rows = list(reader)
    if not 1 <= len(rows) <= 5000:
        raise RuntimeError("SQL callback membership exceeds its bounded page contract")
    tasks = {
        task["task_id"]
        for task in cast(list[dict[str, object]], manifest.to_document()["tasks"])
    }
    seen = set()
    for row in rows:
        if None in row or any(value is None for value in row.values()):
            raise RuntimeError("SQL callback row is incomplete")
        result = load_task_result(row["result_json"])
        if result.run_id != manifest.run_id or result.task_id not in tasks:
            raise RuntimeError("SQL callback result belongs to another pinned run")
        if result.result_sha256 != row["result_sha256"]:
            raise RuntimeError(
                "SQL callback body differs from its immutable result hash"
            )
        if (result.task_id, result.attempt_id) in seen:
            raise RuntimeError("Duplicate SQL callback result membership")
        seen.add((result.task_id, result.attempt_id))
        read_task_result(
            fs,
            _result_path(result),
            run_id=result.run_id,
            task_id=result.task_id,
            attempt_id=result.attempt_id,
            expected_sha256=row["result_sha256"],
        )
        _pipeline_activity_scope(row["job_location"])
        _token(row["job_evidence_sha256"], "service evidence hash", r"[0-9a-f]{64}")
        if row["observed_job_status"] not in {"Completed", "Failed", "Cancelled"} or (
            result.status == "SUCCEEDED" and row["observed_job_status"] != "Completed"
        ):
            raise RuntimeError(
                "SQL callback service evidence is not terminal or successful"
            )


def reconcile_bronze_pipeline(
    spark: Any,
    fs: FileSystem,
    manifest: VerifiedRunManifest,
    dispatch: VerifiedPipelineDispatch,
    *,
    client: ActivityClient,
    observed_utc: str,
    ingestion: SilverRuntime,
    file_arrivals: Mapping[str, SealedFileArrival] | None = None,
) -> dict[str, object]:
    """Seal observed Copy outputs and persist results before any SQL callback."""
    evidence = pipeline_activity_evidence(
        client, manifest, dispatch, observed_utc=observed_utc, allow_unresolved=True
    )
    document = manifest.to_document()
    configuration = cast(dict[str, Any], document["configuration"])
    tasks = {
        task["task_id"]: task
        for task in cast(list[dict[str, Any]], document["tasks"])
        if task["layer"] == "bronze"
    }
    objects = {obj["id"]: obj for obj in configuration["objects"]}
    feeds = {feed["id"]: feed for feed in configuration["feeds"]}
    results = []
    undispatched = dispatch.to_document().get("undispatched_task_ids", [])
    failed = bool(undispatched)
    unresolved = []
    for attempt in cast(list[dict[str, Any]], dispatch.to_document()["attempts"]):
        task = tasks[attempt["task_id"]]
        service = evidence[task["task_id"]]
        if service["observed_job_status"] == "Unresolved":
            unresolved.append({**attempt, "error_code": service["error_code"]})
            failed = True
            continue
        if service["observed_job_status"] == "Completed" and feeds[task["feed_id"]][
            "source_type"
        ] in ("ftp", "sftp"):
            # A landed size match cannot prove that the remote source stayed unchanged.
            unresolved.append(
                {**attempt, "error_code": "COPY_SOURCE_REOBSERVATION_UNAVAILABLE"}
            )
            failed = True
            continue
        status = "SUCCEEDED"
        metrics: dict[str, int] = {}
        refs = []
        error_code = None
        if service["observed_job_status"] != "Completed":
            status = (
                "CANCELLED"
                if service["observed_job_status"] == "Cancelled"
                else "FAILED"
            )
            error_code = "COPY_" + status
        else:
            obj, feed = objects[task["object_id"]], feeds[task["feed_id"]]
            try:
                receipt = _sealed_bronze_table(fs, feed, task, obj, file_arrivals)
                context = {
                    "metadata_mode": "manifest_v1",
                    "raw_source_files": {
                        obj["name"]: [file["path"] for file in receipt["files"]]
                    },
                    "raw_source_sizes": {
                        obj["name"]: [file["size_bytes"] for file in receipt["files"]]
                    },
                    "raw_targets": {obj["name"]: task["target_path"]},
                }
                fingerprint, rows = ingestion.snapshot_fingerprint(
                    ingestion.read_raw(spark, fs, context, obj)
                )
                _write_immutable(
                    fs,
                    _manifest_path(
                        f"Files/orchestration/{manifest.run_id}/bronze_tasks/"
                        f"{task['task_id']}/{attempt['attempt_id']}.json",
                        manifest.run_id,
                    ),
                    _canonical(
                        {
                            **receipt,
                            "run_id": manifest.run_id,
                            "task_id": task["task_id"],
                            "attempt_id": attempt["attempt_id"],
                            "source_batch_id": task["source_batch_id"],
                            "record_multiset_sha256": fingerprint,
                            "rows": rows,
                        }
                    ),
                    MAX_MANIFEST_BYTES,
                )
                refs = [
                    {
                        "kind": "raw_batch",
                        "location": task["target_path"],
                        "version": None,
                        "sha256": fingerprint,
                    }
                ]
                metrics = {
                    "rows_read": rows,
                    "rows_written": rows,
                    "rows_quarantined": 0,
                }
            except ingestion._operation_errors() as error:
                status, error_code = "FAILED", "BRONZE_" + type(error).__name__.upper()
        result = make_task_result(
            manifest,
            task["task_id"],
            attempt_id=attempt["attempt_id"],
            fence=attempt["fence"],
            job_id=service["job_id"],
            status=status,
            finished_utc=service["finished_utc"],
            artifact_refs=refs,
            error_code=error_code,
            metrics=metrics,
        )
        persist_task_result(fs, result)
        results.append((result, service))
        failed = failed or status != "SUCCEEDED"
    recovery = _canonical(
        {
            "format_version": 1,
            "run_id": manifest.run_id,
            "version_id": manifest.version_id,
            "snapshot_sha256": manifest.snapshot_sha256,
            "dispatch_sha256": dispatch.dispatch_sha256,
            "observed_utc": _observation_time(observed_utc)
            .isoformat(timespec="microseconds")
            .replace("+00:00", "Z"),
            "undispatched_task_ids": undispatched,
            "unresolved_attempts": unresolved,
            "terminal_results": [
                {
                    "task_id": result.task_id,
                    "attempt_id": result.attempt_id,
                    "result_sha256": result.result_sha256,
                }
                for result, _ in results
            ],
        }
    )
    recovery_sha256 = hashlib.sha256(recovery.encode("ascii")).hexdigest()
    recovery_path = _manifest_path(
        f"Files/orchestration/{manifest.run_id}/bronze_recovery/{recovery_sha256}.json",
        manifest.run_id,
    )
    _write_immutable(fs, recovery_path, recovery, MAX_MANIFEST_BYTES)
    return {
        "run_id": manifest.run_id,
        "has_failed": failed,
        "undispatched_count": len(undispatched),
        "unresolved_count": len(unresolved),
        "recovery_path": recovery_path,
        "recovery_sha256": recovery_sha256,
        "callbacks": write_pipeline_callbacks(fs, manifest, results),
    }


@dataclass(frozen=True)
class VerifiedLayerDispatch:
    run_id: str
    layer: str
    dispatch_json: str

    @property
    def dispatch_sha256(self) -> str:
        return hashlib.sha256(self.dispatch_json.encode("ascii")).hexdigest()

    def to_document(self) -> dict[str, object]:
        return cast(dict[str, object], json.loads(self.dispatch_json))


def make_layer_dispatch(
    manifest: VerifiedRunManifest, layer: str, attempts: Sequence[dict[str, object]]
) -> VerifiedLayerDispatch:
    if layer not in {"silver", "gold"} or not isinstance(attempts, (list, tuple)):
        raise RuntimeError("A prepared notebook-layer dispatch is required")
    document = manifest.to_document()
    tasks = {
        task["task_id"]: task
        for task in cast(list[dict[str, object]], document["tasks"])
        if task["layer"] == layer
    }
    if not tasks or len(attempts) != len(tasks):
        raise RuntimeError("Layer dispatch omits pinned task membership")
    checked = []
    seen, attempt_ids = set(), set()
    for attempt in attempts:
        if not isinstance(attempt, dict) or set(attempt) != {
            "task_id",
            "attempt_id",
            "fence",
            "source_fence",
        }:
            raise RuntimeError("Invalid prepared-attempt dispatch fields")
        task_id = _token(attempt["task_id"], "prepared task identity", r"[0-9a-f]{64}")
        identity = _attempt_identity(attempt["attempt_id"])
        if task_id not in tasks or task_id in seen or identity in attempt_ids:
            raise RuntimeError("Duplicate or unselected prepared attempt")
        fence = _positive_fence(attempt["fence"])
        source_fence = attempt["source_fence"]
        if tasks[task_id]["source_lock_key"] is not None:
            source_fence = _positive_fence(source_fence)
        elif source_fence is not None:
            raise RuntimeError("A feed-independent task cannot invent a source fence")
        seen.add(task_id)
        attempt_ids.add(identity)
        checked.append(
            {
                "task_id": task_id,
                "attempt_id": identity,
                "fence": fence,
                "source_fence": source_fence,
            }
        )
    payload = {
        "format_version": 1,
        "run_id": manifest.run_id,
        "version_id": manifest.version_id,
        "snapshot_sha256": manifest.snapshot_sha256,
        "layer": layer,
        "attempts": checked,
    }
    return VerifiedLayerDispatch(manifest.run_id, layer, _canonical(payload))


def notebook_layer_dispatch(
    manifest: VerifiedRunManifest, pipeline: VerifiedPipelineDispatch
) -> VerifiedLayerDispatch:
    checked = make_pipeline_dispatch(manifest, pipeline.to_document())
    if checked.dispatch_sha256 != pipeline.dispatch_sha256 or pipeline.layer not in {
        "silver",
        "gold",
    }:
        raise RuntimeError("Notebook dispatch differs from authoritative preparation")
    return make_layer_dispatch(
        manifest,
        pipeline.layer,
        [
            {
                key: attempt[key]
                for key in ("task_id", "attempt_id", "fence", "source_fence")
            }
            for attempt in cast(
                list[dict[str, object]], pipeline.to_document()["attempts"]
            )
        ],
    )


def write_layer_dispatch(fs: FileSystem, dispatch: VerifiedLayerDispatch) -> str:
    path = _manifest_path(
        f"Files/orchestration/{dispatch.run_id}/dispatch/{dispatch.layer}_{dispatch.dispatch_sha256}.json",
        dispatch.run_id,
    )
    return _write_immutable(fs, path, dispatch.dispatch_json, MAX_MANIFEST_BYTES)


def read_layer_dispatch(
    fs: FileSystem,
    path: str,
    manifest: VerifiedRunManifest,
    *,
    layer: str,
    expected_sha256: str,
) -> VerifiedLayerDispatch:
    text = _read_bounded(fs, _manifest_path(path, manifest.run_id), MAX_MANIFEST_BYTES)
    try:
        document = json.loads(text)
        canonical = _canonical(document)
    except (TypeError, ValueError):
        raise RuntimeError("Layer dispatch is not finite JSON") from None
    if hashlib.sha256(canonical.encode("ascii")).hexdigest() != expected_sha256:
        raise RuntimeError("Layer dispatch hash differs from its immutable reference")
    if (
        not isinstance(document, dict)
        or set(document)
        != {
            "format_version",
            "run_id",
            "version_id",
            "snapshot_sha256",
            "layer",
            "attempts",
        }
        or document["format_version"] != 1
        or document["run_id"] != manifest.run_id
        or document["version_id"] != manifest.version_id
        or document["snapshot_sha256"] != manifest.snapshot_sha256
        or document["layer"] != layer
    ):
        raise RuntimeError("Layer dispatch identity differs from the pinned run")
    return make_layer_dispatch(manifest, layer, document["attempts"])


def prepare_silver_contexts(
    fs: FileSystem,
    manifest: VerifiedRunManifest,
    *,
    load_configuration: Callable[[object], PinnedConfiguration],
    file_arrivals: Mapping[str, SealedFileArrival] | None = None,
) -> dict[str, dict[str, str]]:
    tasks = cast(list[dict[str, object]], manifest.to_document()["tasks"])
    feeds = sorted(
        {str(task["feed_id"]) for task in tasks if task["layer"] == "silver"}
    )
    references = {}
    for feed_id in feeds:
        receipt = seal_bronze_receipt(
            fs, manifest, feed_id, file_arrivals=file_arrivals
        )
        write_bronze_receipt(fs, receipt)
        context = source_runtime_config(
            manifest, receipt, load_configuration=load_configuration
        )
        text = _canonical(context)
        digest = hashlib.sha256(text.encode("ascii")).hexdigest()
        path = _manifest_path(
            f"Files/orchestration/{manifest.run_id}/contexts/{digest}.json",
            manifest.run_id,
        )
        _write_immutable(fs, path, text, MAX_MANIFEST_BYTES)
        references[feed_id] = {"path": path, "sha256": digest}
    return references


def build_silver_dag(
    manifest: VerifiedRunManifest,
    contexts: Mapping[str, dict[str, str]],
    dispatch: VerifiedLayerDispatch,
    *,
    manifest_path: str,
    dispatch_path: str,
    lakehouse: str,
    concurrency: int = 2,
    timeout_seconds: int = 3600,
    cell_timeout_seconds: int = 900,
) -> dict[str, object]:
    if type(concurrency) is not int or not 1 <= concurrency <= 16:
        raise RuntimeError("Notebook concurrency must be between 1 and 16")
    if any(
        type(value) is not int or not 1 <= value <= 86400
        for value in (timeout_seconds, cell_timeout_seconds)
    ):
        raise RuntimeError("Notebook timeouts require bounded positive seconds")
    _manifest_path(manifest_path, manifest.run_id)
    _manifest_path(dispatch_path, manifest.run_id)
    prepared = dispatch.to_document()
    verified = make_layer_dispatch(manifest, "silver", prepared["attempts"])
    if verified.dispatch_sha256 != dispatch.dispatch_sha256:
        raise RuntimeError("Prepared layer dispatch differs from the pinned run")
    document = manifest.to_document()
    config = cast(dict[str, object], document["configuration"])
    tasks = [
        task
        for task in cast(list[dict[str, object]], document["tasks"])
        if task["layer"] == "silver"
    ]
    objects = {
        obj["id"]: obj for obj in cast(list[dict[str, object]], config["objects"])
    }
    workers = {
        worker["id"]: worker
        for worker in cast(list[dict[str, object]], config["workers"])
    }
    selected = {task["task_id"] for task in tasks}
    names = {identity: "task_" + str(identity) for identity in selected}
    targets = set()
    activities = []
    for task in tasks:
        obj = objects[task["object_id"]]
        worker = workers.get(task["worker_id"])
        if (
            worker is None
            or obj["lakehouse"] != lakehouse
            or worker["lakehouse"] != lakehouse
        ):
            raise RuntimeError(
                "Silver worker/default Lakehouse binding differs from its parent"
            )
        if task["target_lock_key"] in targets:
            raise RuntimeError("A DAG cannot run multiple writers for the same target")
        targets.add(task["target_lock_key"])
        reference = contexts.get(str(task["feed_id"]))
        if not isinstance(reference, dict) or set(reference) != {"path", "sha256"}:
            raise RuntimeError("A prepared immutable source context is missing")
        _manifest_path(reference["path"], manifest.run_id)
        _token(reference["sha256"], "source context hash", r"[0-9a-f]{64}")
        dependencies = []
        for dependency in cast(list[dict[str, object]], task["dependencies"]):
            parent = dependency["task_id"]
            if parent in selected:
                if dependency["condition"] != "succeeded":
                    raise RuntimeError(
                        "Silver same-layer dependencies require staged-success readiness"
                    )
                dependencies.append(names[parent])
        activities.append(
            {
                "name": names[task["task_id"]],
                "path": worker["notebook_name"],
                "timeoutPerCellInSeconds": cell_timeout_seconds,
                "retry": 0,
                "args": {
                    "run_id": manifest.run_id,
                    "version_id": manifest.version_id,
                    "manifest_path": manifest_path,
                    "snapshot_sha256": manifest.snapshot_sha256,
                    "context_path": reference["path"],
                    "context_sha256": reference["sha256"],
                    "dispatch_path": dispatch_path,
                    "dispatch_sha256": dispatch.dispatch_sha256,
                    "task_id": task["task_id"],
                    "worker_id": worker["id"],
                },
                "dependencies": dependencies,
            }
        )
    return {
        "activities": activities,
        "timeoutInSeconds": timeout_seconds,
        "concurrency": concurrency,
    }


def run_notebook_dag(
    runner: NotebookRunner, dag: dict[str, object], dispatch: VerifiedLayerDispatch
) -> dict[str, object]:
    activities = dag.get("activities")
    if (
        dispatch.layer not in {"silver", "gold"}
        or not isinstance(activities, list)
        or not activities
    ):
        raise RuntimeError("Notebook DAG requires its prepared worker dispatch")
    expected = {}
    seen_tasks = set()
    for activity in activities:
        if not isinstance(activity, dict) or not isinstance(activity.get("args"), dict):
            raise TypeError("Notebook DAG has invalid worker arguments")
        name, args = activity.get("name"), activity["args"]
        task_id = args.get("task_id")
        if (
            not isinstance(name, str)
            or not isinstance(task_id, str)
            or name in expected
            or task_id in seen_tasks
            or args.get("run_id") != dispatch.run_id
            or args.get("dispatch_sha256") != dispatch.dispatch_sha256
        ):
            raise RuntimeError("Notebook DAG differs from its prepared worker dispatch")
        attempt = _dispatch_attempt(dispatch, task_id)
        expected[name] = {
            "task_id": task_id,
            "stage_path": _silver_stage_path(
                dispatch.run_id, task_id, str(attempt["attempt_id"])
            )
            if dispatch.layer == "silver"
            else _manifest_path(
                f"Files/orchestration/{dispatch.run_id}/gold/{task_id}/{attempt['attempt_id']}.json",
                dispatch.run_id,
            ),
        }
        seen_tasks.add(task_id)
    if seen_tasks != {
        attempt["task_id"]
        for attempt in cast(list[dict[str, object]], dispatch.to_document()["attempts"])
    }:
        raise RuntimeError("Notebook DAG omits prepared worker membership")
    if runner.validateDAG(dag) is not True:
        raise RuntimeError("Native notebook DAG validation did not succeed")
    results = runner.runMultiple(dag)
    if not isinstance(results, dict) or set(results) != set(expected):
        raise RuntimeError("Notebook DAG result membership is incomplete or ambiguous")
    for name, value in results.items():
        if (
            not isinstance(value, dict)
            or "exception" not in value
            or value["exception"] is not None
            or not isinstance(value.get("exitVal"), str)
        ):
            raise RuntimeError(
                "Notebook DAG failed or returned ambiguous worker status"
            )
        try:
            acknowledgement = json.loads(value["exitVal"])
        except ValueError:
            raise RuntimeError(
                "Notebook worker acknowledgement is not valid JSON"
            ) from None
        if acknowledgement != expected[name]:
            raise RuntimeError(
                "Notebook worker acknowledgement differs from its attempt"
            )
    return results


def _silver_stage_path(run_id: str, task_id: str, attempt_id: str) -> str:
    return _manifest_path(
        f"Files/orchestration/{run_id}/silver/{task_id}/{attempt_id}.json", run_id
    )


def _read_json_metadata(fs: FileSystem, path: str, max_bytes: int) -> dict[str, Any]:
    text = _read_bounded(fs, path, max_bytes)
    try:
        value = json.loads(text)
        _canonical(value)
    except (ValueError, TypeError):
        raise RuntimeError("Persisted task metadata is not finite JSON") from None
    if not isinstance(value, dict):
        raise TypeError("Persisted task metadata must be an object")
    return value


def _dispatch_attempt(
    dispatch: VerifiedLayerDispatch, task_id: str
) -> dict[str, object]:
    matches = [
        attempt
        for attempt in cast(list[dict[str, object]], dispatch.to_document()["attempts"])
        if attempt["task_id"] == task_id
    ]
    if len(matches) != 1:
        raise RuntimeError("Prepared worker attempt is missing or ambiguous")
    return matches[0]


def initialize_silver_sources(
    spark: Any,
    fs: FileSystem,
    contexts: Mapping[str, dict[str, str]],
    *,
    lakehouse: str,
    ingestion: SilverRuntime,
) -> None:
    """Create shared namespaces and seal parent populations before parallel writers."""
    for reference in contexts.values():
        text = _read_bounded(fs, reference["path"], MAX_MANIFEST_BYTES)
        context = ingestion.manifest_processing_context(
            text, reference["sha256"], expected_lakehouse=lakehouse
        )
        _verify_context_file_membership(fs, context)
        ingestion.setup_namespaces(spark, context)
        ingestion.inventory(
            spark, fs, context, table_names=list(context["raw_source_files"])
        )


def execute_silver_task(
    spark: Any,
    fs: FileSystem,
    manifest: VerifiedRunManifest,
    dispatch: VerifiedLayerDispatch,
    *,
    task_id: str,
    worker_id: str,
    context_path: str,
    context_sha256: str,
    lakehouse: str,
    code_sha256: str,
    ingestion: SilverRuntime,
) -> str:
    resolved = resolve_worker_task(
        manifest,
        task_id,
        worker_id=worker_id,
        code_sha256=code_sha256,
        lakehouse=lakehouse,
    )
    checked_dispatch = make_layer_dispatch(
        manifest, "silver", dispatch.to_document()["attempts"]
    )
    if checked_dispatch.dispatch_sha256 != dispatch.dispatch_sha256:
        raise RuntimeError("Silver dispatch differs from its pinned worker context")
    attempt = _dispatch_attempt(dispatch, task_id)
    context_path = _manifest_path(context_path, manifest.run_id)
    context = ingestion.manifest_processing_context(
        _read_bounded(fs, context_path, MAX_MANIFEST_BYTES),
        context_sha256,
        expected_lakehouse=lakehouse,
    )
    task, obj = (
        cast(dict[str, Any], resolved["task"]),
        cast(dict[str, Any], resolved["object"]),
    )
    if (
        context["metadata_run_id"] != manifest.run_id
        or context["metadata_version"] != manifest.version_id
        or context["source_feed_id"] != task["feed_id"]
        or context["source_batch_id"] != task["source_batch_id"]
        or obj["name"] not in context["selected_tables"]
    ):
        raise RuntimeError("Worker source context differs from the prepared task")
    path = _silver_stage_path(manifest.run_id, task_id, str(attempt["attempt_id"]))
    identity = {
        "format_version": 1,
        "run_id": manifest.run_id,
        "version_id": manifest.version_id,
        "task_id": task_id,
        "object_id": obj["id"],
        "attempt_id": attempt["attempt_id"],
        "fence": attempt["fence"],
        "source_fence": attempt["source_fence"],
        "source_batch_id": task["source_batch_id"],
        "context_sha256": context_sha256,
        "code_sha256": code_sha256,
        "target_table": task["target_path"],
    }
    if fs.exists(path):
        existing = _read_json_metadata(fs, path, MAX_RESULT_BYTES)
        if (
            any(existing.get(name) != value for name, value in identity.items())
            or existing.get("status") != "SUCCEEDED"
        ):
            raise RuntimeError(
                "A failed or changed staged task requires explicit recovery"
            )
        return path
    try:
        _verify_context_file_membership(fs, context)
        results = ingestion.load_silver(
            spark, fs, context, table_names=[str(obj["name"])]
        )
        if len(results) != 1 or results[0]["status"] != "SUCCESS":
            raise RuntimeError("Silver object did not complete with passing quality")
        version = ingestion.delta_version(spark, str(task["target_path"]))
        stage = {
            **identity,
            "status": "SUCCEEDED",
            "delta_version": version,
            "source_rows": results[0]["source_rows"],
            "silver_rows": results[0]["silver_rows"],
            "quarantine_rows": results[0]["quarantine_rows"],
            "error_code": None,
        }
    except ingestion._operation_errors() as error:
        _write_immutable(
            fs,
            path,
            _canonical(
                {
                    **identity,
                    "status": "FAILED",
                    "delta_version": None,
                    "source_rows": None,
                    "silver_rows": None,
                    "quarantine_rows": None,
                    "error_code": "SILVER_" + type(error).__name__.upper(),
                }
            ),
            MAX_RESULT_BYTES,
        )
        raise RuntimeError("Silver task failed; Source commit is withheld") from None
    _write_immutable(fs, path, _canonical(stage), MAX_RESULT_BYTES)
    return path


def verify_silver_stage_outputs(
    spark: Any,
    fs: FileSystem,
    manifest: VerifiedRunManifest,
    dispatch: VerifiedLayerDispatch,
    contexts: Mapping[str, dict[str, str]],
    *,
    lakehouse: str,
    ingestion: SilverRuntime,
) -> dict[str, dict[str, Any]]:
    """Verify every immutable object output before publication or SQL reconciliation."""
    checked = make_layer_dispatch(
        manifest, "silver", dispatch.to_document()["attempts"]
    )
    if checked.dispatch_sha256 != dispatch.dispatch_sha256:
        raise RuntimeError("Silver fan-in dispatch differs from its pinned run")
    tasks = [
        task
        for task in cast(list[dict[str, object]], manifest.to_document()["tasks"])
        if task["layer"] == "silver"
    ]
    if set(contexts) != {str(task["feed_id"]) for task in tasks}:
        raise RuntimeError(
            "Silver source fan-in omits or invents selected feed membership"
        )
    config = cast(dict[str, object], manifest.to_document()["configuration"])
    objects = {
        obj["id"]: obj for obj in cast(list[dict[str, object]], config["objects"])
    }
    workers = {
        worker["id"]: worker
        for worker in cast(list[dict[str, object]], config["workers"])
    }
    if any(objects[task["object_id"]]["lakehouse"] != lakehouse for task in tasks):
        raise RuntimeError("Silver stage evidence belongs to another Lakehouse binding")
    stages = {}
    for task in tasks:
        attempt = _dispatch_attempt(dispatch, str(task["task_id"]))
        stage = _read_json_metadata(
            fs,
            _silver_stage_path(
                manifest.run_id, str(task["task_id"]), str(attempt["attempt_id"])
            ),
            MAX_RESULT_BYTES,
        )
        if (
            stage.get("run_id") != manifest.run_id
            or stage.get("version_id") != manifest.version_id
            or stage.get("task_id") != task["task_id"]
            or stage.get("attempt_id") != attempt["attempt_id"]
            or stage.get("fence") != attempt["fence"]
            or stage.get("source_fence") != attempt["source_fence"]
            or stage.get("source_batch_id") != task["source_batch_id"]
            or stage.get("status") != "SUCCEEDED"
            or stage.get("target_table") != task["target_path"]
            or stage.get("context_sha256") != contexts[str(task["feed_id"])]["sha256"]
            or stage.get("code_sha256")
            != workers[objects[task["object_id"]]["worker_id"]]["code_sha256"]
            or type(stage.get("delta_version")) is not int
            or stage["delta_version"]
            != ingestion.delta_version(spark, str(task["target_path"]))
            or any(
                type(stage.get(name)) is not int or stage[name] < 0
                for name in ("source_rows", "silver_rows", "quarantine_rows")
            )
            or stage["source_rows"] != stage["silver_rows"] + stage["quarantine_rows"]
        ):
            raise RuntimeError(
                "Silver staged output is missing, stale or failed; publication is withheld"
            )
        stages[str(task["task_id"])] = stage
    return stages


def finalize_silver_sources(
    spark: Any,
    fs: FileSystem,
    manifest: VerifiedRunManifest,
    dispatch: VerifiedLayerDispatch,
    contexts: Mapping[str, dict[str, str]],
    *,
    lakehouse: str,
    ingestion: SilverRuntime,
) -> dict[str, object]:
    """Only existing source validation publishes commits after complete stage fan-in."""
    verify_silver_stage_outputs(
        spark,
        fs,
        manifest,
        dispatch,
        contexts,
        lakehouse=lakehouse,
        ingestion=ingestion,
    )
    committed = []
    for feed_id, reference in contexts.items():
        context = ingestion.manifest_processing_context(
            _read_bounded(fs, reference["path"], MAX_MANIFEST_BYTES),
            reference["sha256"],
            expected_lakehouse=lakehouse,
        )
        if (
            context["metadata_run_id"] != manifest.run_id
            or context["source_feed_id"] != feed_id
        ):
            raise RuntimeError("Source commit context differs from the pinned run")
        _verify_context_file_membership(fs, context)
        ingestion.validate_source(spark, fs, context)
        committed.append(
            {"feed_id": feed_id, "source_batch_id": context["source_batch_id"]}
        )
    return {
        "run_id": manifest.run_id,
        "status": "SOURCE_COMMITTED",
        "sources": committed,
    }


def reconcile_silver_pipeline(
    spark: Any,
    fs: FileSystem,
    manifest: VerifiedRunManifest,
    pipeline: VerifiedPipelineDispatch,
    *,
    client: ActivityClient,
    observed_utc: str,
    lakehouse: str,
    ingestion: SilverRuntime,
    load_configuration: Callable[[object], PinnedConfiguration],
    file_arrivals: Mapping[str, SealedFileArrival] | None = None,
) -> dict[str, object]:
    evidence = pipeline_activity_evidence(
        client,
        manifest,
        pipeline,
        observed_utc=observed_utc,
        parent_activity="orchestrate_silver",
    )
    dispatch = notebook_layer_dispatch(manifest, pipeline)
    stages, commits = {}, {}
    failure_code = None
    if any(value["observed_job_status"] != "Completed" for value in evidence.values()):
        failure_code = "SILVER_PARENT_FAILED"
    else:
        try:
            contexts = prepare_silver_contexts(
                fs,
                manifest,
                load_configuration=load_configuration,
                file_arrivals=file_arrivals,
            )
            stages = verify_silver_stage_outputs(
                spark,
                fs,
                manifest,
                dispatch,
                contexts,
                lakehouse=lakehouse,
                ingestion=ingestion,
            )
            for feed_id, reference in contexts.items():
                context = ingestion.manifest_processing_context(
                    _read_bounded(fs, reference["path"], MAX_MANIFEST_BYTES),
                    reference["sha256"],
                    expected_lakehouse=lakehouse,
                )
                ingestion.cleanup_snapshots(spark, fs, context)
                commits[feed_id] = ingestion.source_commit_evidence(spark, context)
        except ingestion._operation_errors():
            failure_code = "SILVER_OUTPUT_OR_COMMIT_INVALID"
    tasks = {
        task["task_id"]: task
        for task in cast(list[dict[str, Any]], manifest.to_document()["tasks"])
        if task["layer"] == "silver"
    }
    results = []
    for attempt in cast(list[dict[str, Any]], pipeline.to_document()["attempts"]):
        task, service = tasks[attempt["task_id"]], evidence[attempt["task_id"]]
        refs, metrics, readiness = [], {}, []
        if failure_code is None:
            stage = stages[task["task_id"]]
            refs = [
                {
                    "kind": "delta_version",
                    "location": task["target_path"],
                    "version": stage["delta_version"],
                    "sha256": hashlib.sha256(
                        _canonical(stage).encode("ascii")
                    ).hexdigest(),
                },
                commits[task["feed_id"]],
            ]
            metrics = {
                "rows_read": stage["source_rows"],
                "rows_written": stage["silver_rows"],
                "rows_quarantined": stage["quarantine_rows"],
            }
            readiness = ["succeeded", "source_committed"]
        result = make_task_result(
            manifest,
            task["task_id"],
            attempt_id=attempt["attempt_id"],
            fence=attempt["fence"],
            job_id=service["job_id"],
            status="SUCCEEDED" if failure_code is None else "FAILED",
            finished_utc=service["finished_utc"],
            artifact_refs=refs,
            metrics=metrics,
            readiness=readiness,
            error_code=failure_code,
        )
        persist_task_result(fs, result)
        results.append((result, service))
    return {
        "run_id": manifest.run_id,
        "has_failed": failure_code is not None,
        "callbacks": write_pipeline_callbacks(fs, manifest, results),
    }
