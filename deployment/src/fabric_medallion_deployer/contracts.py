"""Compile the source registry and authoritative schemas without cloud access."""

from __future__ import annotations

import csv
import hashlib
import json
import re
import subprocess
import sys
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .asset_paths import (
    resolve_asset_path,
    source_registry_path,
    source_registry_root,  # noqa: F401 -- public compatibility export
    source_specifications_root,
    synthetic_data_root,
    use_case_descriptor,
)

STEPS = {
    "setup": "setup_and_namespace_validation",
    "inventory": "bronze_csv_inventory",
    "silver": "bronze_csv_to_silver_delta",
    "validation": "bronze_silver_validation",
    "cleanup": "bronze_snapshot_cleanup",
}
RESERVED_COLUMNS = frozenset(
    {
        "ingest_batch_run_id",
        "ingest_processing_date",
        "ingest_extract_date",
        "ingest_loaded_utc",
        "ingest_source_system",
        "ingest_source_file_name",
        "ingest_contract_hash",
        "ingest_corrupt_record",
        "dq_passed",
        "dq_failure_reason",
        "dq_warning",
        "_corrupt_record",
        "_raw_record",
        "_dq_reasons",
        "record_id",
        "record_key_hash",
        "record_hash",
        "record_previous_hash",
        "record_version",
        "record_change_type",
        "record_observed_utc",
        "record_source_path",
        "record_batch_hash",
    }
)
SOURCE_TYPES = frozenset(
    {
        "int",
        "bigint",
        "money",
        "decimal",
        "float",
        "bit",
        "boolean",
        "datetime",
        "uuid",
        "varchar",
    }
)
IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def identifier(value: str, *, lowercase: bool = False) -> str:
    """Validate metadata identifiers before constructing SQL or Spark paths."""
    if not IDENTIFIER.fullmatch(value) or (lowercase and value != value.lower()):
        raise ValueError(
            f"Invalid {'lowercase ' if lowercase else ''}identifier: {value!r}"
        )
    return value


def digest(path: Path) -> str:
    """Hash a local artifact without materializing large files in memory."""
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def local_path(root: Path, relative: str) -> Path:
    """Resolve a portable registry path strictly inside its owning root."""
    path = (root / Path(*relative.replace("\\", "/").split("/"))).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ValueError(f"Asset path escapes its root: {relative!r}")
    return path


@dataclass(frozen=True)
class Source:
    """Validated source binding with schema metadata at the JSON boundary."""

    code: str
    database: str
    source_schema: str
    silver_schema: str
    quarantine_schema: str
    connection: str
    pipeline: str
    lakehouse: str
    gold_lakehouse: str
    tables: tuple[dict[str, Any], ...]
    schema_path: Path
    manifest_path: Path
    manifest: dict[str, Any]
    contract_hash: str

    @property
    def notebooks(self) -> dict[str, str]:
        return {step: f"nb_{self.code}_{suffix}" for step, suffix in STEPS.items()}

    @property
    def bronze_root(self) -> str:
        return f"Files/bronze/{self.code}"

    def runtime_config(self) -> dict[str, Any]:
        """Return a self-contained, credential-free notebook contract."""
        return {
            "source_code": self.code,
            "lakehouse": self.lakehouse,
            "silver_schema": self.silver_schema,
            "quarantine_schema": self.quarantine_schema,
            "bronze_root": self.bronze_root,
            "contract_hash": self.contract_hash,
            "seed_snapshot_date": self.manifest["snapshot_date"],
            "tables": deepcopy(list(self.tables)),
        }

    def seed_file(self, table_name: str) -> Path:
        entry = next(t for t in self.manifest["tables"] if t["name"] == table_name)
        return local_path(self.manifest_path.parent, entry["file"])


def _validate_tables(tables: list[dict[str, Any]]) -> None:
    inventory = {table["name"]: table for table in tables}
    if len(inventory) != len(tables):
        raise ValueError("Duplicate source table")
    for table in tables:
        identifier(table["name"], lowercase=True)
        names = [identifier(col["name"]) for col in table["columns"]]
        if len({name.casefold() for name in names}) != len(names):
            raise ValueError(f"{table['name']}: case-insensitive duplicate fields")
        if RESERVED_COLUMNS.intersection(name.casefold() for name in names) or any(
            name.startswith("_") for name in names
        ):
            raise ValueError(f"{table['name']}: reserved ingestion/DQ field collision")
        if not table["keys"] or not set(table["keys"]) <= set(names):
            raise ValueError(f"{table['name']}: missing business key")
        for column in table["columns"]:
            if column["data_type"] not in SOURCE_TYPES:
                raise ValueError(
                    f"{table['name']}: unsupported type {column['data_type']}"
                )
            if not isinstance(column["nullable"], bool):
                raise TypeError(f"{table['name']}: invalid nullability")
            if column["data_type"] == "decimal":
                precision, scale = column["precision"], column["scale"]
                if not 0 <= scale <= precision <= 38:
                    raise ValueError(
                        f"{table['name']}: invalid decimal precision/scale"
                    )
        for relation in table["foreign_keys"]:
            target = inventory.get(relation["table"])
            if (
                target is None
                or not relation["columns"]
                or len(relation["columns"]) != len(relation["target_columns"])
                or not set(relation["columns"]) <= set(names)
                or not set(relation["target_columns"])
                <= {c["name"] for c in target["columns"]}
            ):
                raise ValueError(f"{table['name']}: invalid foreign-key contract")


def _validate_lineage(
    root: Path,
    schema: dict[str, Any],
    manifest: dict[str, Any],
    schema_path: Path,
    use_case: str | None = None,
) -> None:
    if manifest["synthetic_only"] is not True or schema["synthetic_only"] is not True:
        raise ValueError(
            "Only declared synthetic source assets can seed demo databases"
        )
    if digest(schema_path) != manifest["schema_sha256"]:
        raise ValueError(f"Schema hash mismatch: {schema_path}")
    descriptor = use_case_descriptor(root, use_case)
    declared = descriptor.get("provenance_assets", {})
    fingerprints = {
        "generator_sha256", "contract_builder_sha256", "validator_sha256",
        "department_profile_sha256",
    }
    if (fingerprints & manifest.keys()) - declared.keys():
        raise ValueError("Manifest provenance has no declared asset binding")
    for key, relative in declared.items():
        path = resolve_asset_path(root, relative, descriptor["id"])
        if digest(path) != manifest[key]:
            raise ValueError(
                f"{path.name} changed; explicitly rebaseline or regenerate and validate the snapshots"
            )
    if "extraction_manifest_sha256" not in schema:
        return
    extracted = source_specifications_root(root, descriptor["id"]) / "extracted"
    extraction_path = extracted / "extraction_manifest.json"
    if digest(extraction_path) != schema["extraction_manifest_sha256"]:
        raise ValueError("Extraction manifest hash mismatch")
    extraction = json.loads(extraction_path.read_text(encoding="utf-8"))
    evidence = extraction["sources"][schema["source"]]
    for name, key in (
        ("pdf", "sha256"),
        ("markdown", "markdown_sha256"),
        ("structured_tables", "tables_sha256"),
        ("native_text", "native_sha256"),
    ):
        parent = extracted.parent if name == "pdf" else extracted
        if digest(local_path(parent, evidence[name])) != evidence[key]:
            raise ValueError(f"Source extraction lineage mismatch: {evidence[name]}")
    if evidence["sha256"] != schema["source_pdf_sha256"]:
        raise ValueError("Source PDF hash mismatch")


def load_sources(root: Path, use_case: str | None = None) -> tuple[Source, ...]:
    """Validate the complete inventory and compile portable source bindings."""
    registry = json.loads(
        source_registry_path(root, use_case).read_text(
            encoding="utf-8"
        )
    )
    if registry["version"] != 1:
        raise ValueError("Unsupported source registry version")
    lakehouses = registry["lakehouses"]
    if not {"bronze_silver", "gold"} <= lakehouses.keys():
        raise ValueError("Registry must declare Bronze/Silver and Gold Lakehouse bindings")
    for name in lakehouses.values():
        identifier(name, lowercase=True)
    codes = [entry["code"] for entry in registry["sources"]]
    if not codes or len(codes) != len(set(codes)):
        raise ValueError("Expected nonempty, unique source codes")
    descriptor = use_case_descriptor(root, use_case)
    exclusions = descriptor.get("change_hash_exclusions", {})
    if any(
        lakehouses[role] != descriptor["resources"][role]
        for role in ("bronze_silver", "gold")
    ):
        raise ValueError("Registry Lakehouse bindings differ from the selected profile")
    if not set(exclusions) <= set(codes):
        raise ValueError("Change-hash exclusions reference an unknown source")
    result = []
    for entry in registry["sources"]:
        if entry["silver_schema"] == entry["quarantine_schema"]:
            raise ValueError("Silver and quarantine schema bindings must be distinct")
        for field in (
            "code",
            "database",
            "source_schema",
            "silver_schema",
            "quarantine_schema",
            "connection",
            "pipeline",
        ):
            identifier(entry[field], lowercase=True)
        schema_path = resolve_asset_path(root, entry["schema_path"], descriptor["id"])
        manifest_path = resolve_asset_path(root, entry["manifest_path"], descriptor["id"])
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if schema["source"] != entry["code"] or manifest["source"] != entry["code"]:
            raise ValueError("Registry/schema/manifest source identity mismatch")
        _validate_lineage(root, schema, manifest, schema_path, descriptor["id"])
        tables = deepcopy(schema["tables"])
        _validate_tables(tables)
        if not set(exclusions.get(entry["code"], {})) <= {t["name"] for t in tables}:
            raise ValueError("Change-hash exclusions reference an unknown table")
        empty_tables = set(entry["allow_empty_tables"])
        if not empty_tables <= {t["name"] for t in tables}:
            raise ValueError("Unknown allowed-empty table")
        manifest_tables = {t["name"]: t for t in manifest["tables"]}
        if len(manifest_tables) != len(tables) or set(manifest_tables) != {
            t["name"] for t in tables
        }:
            raise ValueError("Schema and manifest table inventories differ")
        files = set()
        for table in tables:
            table["allow_empty"] = table["name"] in empty_tables
            observation_columns = exclusions.get(entry["code"], {}).get(
                table["name"], []
            )
            if not set(observation_columns) <= {c["name"] for c in table["columns"]}:
                raise ValueError(f"{table['name']}: unknown change-hash exclusion")
            table["change_hash_exclusions"] = [
                name
                for name in observation_columns
                if name in {c["name"] for c in table["columns"]}
            ]
            metadata = manifest_tables[table["name"]]
            path = local_path(manifest_path.parent, metadata["file"])
            if path.parent.name != table["name"] or path.suffix != ".csv":
                raise ValueError(f"Unexpected CSV location: {path.name}")
            if digest(path) != metadata["sha256"]:
                raise ValueError(f"Seed CSV hash mismatch: {path.name}")
            with path.open(encoding="utf-8", newline="") as stream:
                reader = csv.DictReader(stream)
                if reader.fieldnames != [c["name"] for c in table["columns"]]:
                    raise ValueError(f"Seed CSV header mismatch: {path.name}")
                rows = sum(1 for _ in reader)
            if rows != metadata["rows"] or metadata["columns"] != len(table["columns"]):
                raise ValueError(f"Seed CSV count mismatch: {path.name}")
            if not rows and not table["allow_empty"]:
                raise ValueError(f"Unexpected empty seed: {path.name}")
            files.add(path)
        if files != {
            p.resolve() for p in (manifest_path.parent / entry["code"]).rglob("*.csv")
        }:
            raise ValueError("Unexpected or missing seed CSV files")
        payload = {**entry, "tables": tables, "lakehouses": lakehouses}
        contract_hash = hashlib.sha256(
            json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
        ).hexdigest()
        result.append(
            Source(
                **{
                    name: entry[name]
                    for name in (
                        "code",
                        "database",
                        "source_schema",
                        "silver_schema",
                        "quarantine_schema",
                        "connection",
                        "pipeline",
                    )
                },
                lakehouse=lakehouses["bronze_silver"],
                gold_lakehouse=lakehouses["gold"],
                tables=tuple(tables),
                schema_path=schema_path,
                manifest_path=manifest_path,
                manifest=manifest,
                contract_hash=contract_hash,
            )
        )
    names = [
        name
        for source in result
        for name in (
            source.database,
            source.pipeline,
            *source.notebooks.values(),
        )
    ]
    if len(names) != len(set(names)):
        raise ValueError("Duplicate owned item name")
    return tuple(result)


def owned_inventory(sources: tuple[Source, ...]) -> dict[str, str]:
    """Return normal reusable inventory; source simulators are prerequisites."""
    from .inventory import reusable_inventory

    return reusable_inventory(sources)


def validate_seed_data(root: Path, use_case: str | None = None) -> None:
    """Run a case's declared local seed validator; cases need not have generators."""
    descriptor = use_case_descriptor(root, use_case)
    module = descriptor.get("seed_validator")
    if module is None:
        return
    module_name = re.compile(r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*")
    if not isinstance(module, str) or not module_name.fullmatch(module):
        raise ValueError("Invalid seed-validator module")
    builder_module = descriptor.get("contract_builder_module", "")
    if not isinstance(builder_module, str) or (
        builder_module and not module_name.fullmatch(builder_module)
    ):
        raise ValueError("Invalid contract-builder module")
    script = (
        "import importlib, sys\n"
        "from pathlib import Path\n"
        "sys.path.insert(0, sys.argv[1])\n"
        "if sys.argv[3]:\n"
        "    contracts = importlib.import_module(sys.argv[3])\n"
        "    contracts.ROOT = Path(sys.argv[1])\n"
        "    contracts.EXTRACTED = Path(sys.argv[2]) / 'extracted'\n"
        "generator = importlib.import_module(sys.argv[4])\n"
        "sys.argv = [generator.__file__, '--validate-only', '--output-root', sys.argv[5]]\n"
        "raise SystemExit(generator.main())\n"
    )
    subprocess.run(
        [
            sys.executable,
            "-B",
            "-c",
            script,
            str(root.resolve()),
            (
                str(source_specifications_root(root, descriptor["id"]))
                if builder_module else ""
            ),
            builder_module,
            module,
            str(synthetic_data_root(root, descriptor["id"])),
        ],
        check=True,
        cwd=root,
    )
