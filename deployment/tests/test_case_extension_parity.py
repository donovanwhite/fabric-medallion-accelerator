# Copyright (c) Microsoft Corporation.
# SPDX-License-Identifier: MIT
"""Pre-wiring parity against OLD trusted code; no cloud, JVM or MD execution.

These are migration/extraction tests, not post-wiring regression or Spark/Delta
integration tests. Keep the old implementation available until they have run.
"""

from __future__ import annotations

import ast
import importlib.abc
import importlib.util
import inspect
import json
import linecache
import subprocess
import tempfile
import unittest
from copy import deepcopy
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fabric_medallion_deployer.contracts import Source

ROOT = Path(__file__).resolve().parents[2]
EXTENSIONS = ROOT / "use_cases" / "treasury" / "extensions"
SEEDS = ROOT / "use_cases" / "treasury" / "fixtures"
PROFILE_PATH = SEEDS / "contracts" / "kzn_departments.json"


def load_file(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


REVISION = "4d76d7f5e3278ca4b16ed6e3e78ae14b4349b194"


class GitBaselineLoader(importlib.abc.SourceLoader):
    """Standard module loading from a read-only, local, trusted Git source."""

    def __init__(self, filename, source):
        self.filename = filename
        self.source = source

    def get_filename(self, fullname):
        return self.filename

    def get_data(self, path):
        return self.source.encode("utf-8")


def load_old(name, relative):
    """Load only versioned Python from the task's pinned local Git revision.

    The path is a hardcoded test constant, never MD/metadata/user expressions.
    No snapshot files are written, and wiring can remove old domain globals.
    """
    source = subprocess.run(
        ["git", "--no-pager", "show", f"{REVISION}:{relative}"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    filename = f"<trusted-git-{REVISION}:{relative}>"
    linecache.cache[filename] = (len(source), None, source.splitlines(True), filename)
    loader = GitBaselineLoader(filename, source)
    spec = importlib.util.spec_from_loader(name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


profile = load_file("case_profile_parity", EXTENSIONS / "gold_profile.py")
rules = load_file("case_source_parity", EXTENSIONS / "source_rules.py")
adapters = load_file("case_gold_parity", EXTENSIONS / "gold_rules.py")
compiler = load_old(
    "fabric_medallion_deployer._old_gold_parity",
    "deployment/src/fabric_treasury_deployer/gold.py",
)
old_rules = load_old("old_source_parity", "artefacts/medallion/treasury_rules.py")
old_gold = load_old("old_gold_parity", "artefacts/gold/treasury_gold.py")


def source_fixture():
    """Existing schemas/seeds without conflating relocation lineage with parity."""
    sources = []
    for code in ("bas", "csd", "persal"):
        folder = SEEDS / f"{code}_synthetic_data"
        sources.append(
            Source(
                code=code,
                database=f"{code}_parity",
                source_schema=code,
                silver_schema=f"slv_{code}",
                quarantine_schema=f"dq_{code}",
                connection=f"{code}_connection",
                pipeline=f"{code}_pipeline",
                lakehouse="lh_parity",
                gold_lakehouse="lh_gold_parity",
                tables=tuple(
                    json.loads((folder / "schema.json").read_text())["tables"]
                ),
                schema_path=folder / "schema.json",
                manifest_path=folder / "manifest.json",
                manifest=json.loads((folder / "manifest.json").read_text()),
                contract_hash="a" * 64,
            )
        )
    return tuple(sources)


def function_ast(function):
    return ast.dump(ast.parse(inspect.getsource(function)), include_attributes=False)


class GoldProfileParityTests(unittest.TestCase):
    def test_exact_fact_blocker_and_dimension_definitions(self):
        self.assertEqual(profile.FACTS, compiler.FACTS)
        self.assertEqual(profile.BLOCKED, compiler.BLOCKED)
        self.assertEqual(profile.dimension_specs(), compiler.dimension_specs())
        self.assertEqual(len(profile.dimension_specs()) + 2, 16)
        self.assertEqual(len(profile.FACTS), 26)
        self.assertEqual(sum(f["name"].startswith("fact_") for f in profile.FACTS), 24)

    def test_exact_mlv_candidate_definitions(self):
        tree = ast.parse(inspect.getsource(compiler.mlv_definitions))
        assignment = next(
            node
            for node in tree.body[0].body
            if isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id == "candidates"
                for target in node.targets
            )
        )
        self.assertEqual(profile.MLV_CANDIDATES, ast.literal_eval(assignment.value))

    def test_full_bootstrap_content_mlv_and_legacy_hash_parity(self):
        sources = source_fixture()
        # Old bootstrap hardcodes its old paths. Only this temporary fixture
        # reconstitutes those paths; no working-tree moves are undone.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            old_profile = root / "artefacts" / "synthetic_data" / "kzn_departments.json"
            runtime = root / "artefacts" / "gold" / "treasury_gold.py"
            old_profile.parent.mkdir(parents=True)
            runtime.parent.mkdir(parents=True)
            old_profile.write_bytes(PROFILE_PATH.read_bytes())
            runtime.write_bytes(
                (ROOT / "artefacts" / "gold" / "gold_runtime.py").read_bytes()
            )
            expected = compiler.bootstrap_gold_config(root, sources)
            self.assertEqual(profile.GOLD_SCHEMA, expected["schema"])
            domain = profile.domain_specs(
                ROOT, sources, department_profile_resolver=lambda root: PROFILE_PATH
            )
            self.assertEqual(domain, {key: expected[key] for key in domain})
            candidate = deepcopy(expected)
            candidate.update(domain)
            candidate["mlvs"] = compiler.mlv_definitions(candidate)
            # Parent retains this algorithm; assert its exact existing output.
            import hashlib

            candidate.pop("contract_hash")
            candidate["contract_hash"] = hashlib.sha256(
                json.dumps(candidate, sort_keys=True).encode()
            ).hexdigest()
            self.assertEqual(candidate, expected)
            self.assertEqual(len(domain["supplier_crosswalk"]), 1000)
            # Returning a domain must not mutate the exported template.
            domain["facts"][0]["attributes"].append("not_a_template_attribute")
            self.assertEqual(profile.FACTS, compiler.FACTS)

    def test_invalid_source_fields_and_missing_mapping_remain_errors(self):
        sources = source_fixture()
        tables = deepcopy(list(sources[0].tables))
        tables[0]["columns"] = [
            c for c in tables[0]["columns"] if c["name"] != "transaction_type"
        ]
        from dataclasses import replace

        invalid = (replace(sources[0], tables=tuple(tables)), *sources[1:])
        with self.assertRaisesRegex(ValueError, "unknown source fields"):
            profile.domain_specs(
                ROOT, invalid, department_profile_resolver=lambda root: PROFILE_PATH
            )
        with self.assertRaises(FileNotFoundError):
            profile.domain_specs(
                ROOT,
                sources,
                department_profile_resolver=lambda root: root / "missing-profile.json",
            )

    def test_declared_asset_resolver_matches_explicit_resolver(self):
        sources = source_fixture()
        self.assertEqual(
            profile.domain_specs(ROOT, sources),
            profile.domain_specs(
                ROOT, sources, department_profile_resolver=lambda root: PROFILE_PATH
            ),
        )


class SourceRuleParityTests(unittest.TestCase):
    def test_every_specialist_function_is_branch_exact(self):
        # AST identity is deliberately stronger than comparing a few samples:
        # every BAS/CSD/PERSAL blocker, null operation and aggregate is retained.
        for name in ("_column", "source_business_checks", "row_rules"):
            with self.subTest(name=name):
                self.assertEqual(
                    function_ast(getattr(rules, name)),
                    function_ast(getattr(old_rules, name)),
                )
        for name in ("BANK_FOREIGN_FIELDS", "BANK_LOCAL_FIELDS", "BANK_COMMON_FIELDS"):
            self.assertEqual(getattr(rules, name), getattr(old_rules, name))
        self.assertFalse(hasattr(rules, "apply_rules"))
        self.assertFalse(hasattr(rules, "validate_rule_specs"))
        self.assertEqual(
            set(rules.RUNTIME_GLOBALS),
            {"row_rules", "source_business_checks", "legacy_context_bindings"},
        )
        for source, silver, quarantine in (
            ("bas", "slv_bas", "quarantine_bas"),
            ("csd", "slv_csd", "quarantine_csd"),
            ("persal", "slv_psl", "quarantine_psl"),
        ):
            self.assertEqual(
                rules.legacy_context_bindings(source),
                {
                    "lakehouse": "lh_brz_slv_treasury",
                    "bronze_root": f"Files/bronze/{source}",
                    "silver_schema": silver,
                    "quarantine_schema": quarantine,
                },
            )

    def test_predicate_expression_parity_for_every_table_branch(self):
        with patch("pyspark.sql.functions", Functions()):
            for name in ("bank_accounts", "t_allow", "t_quali", "t_leave", "other"):
                with self.subTest(table=name):
                    self.assertEqual(
                        repr(canonical(rules.row_rules({"name": name}))),
                        repr(canonical(old_rules.row_rules({"name": name}))),
                    )
            for module in (rules, old_rules):
                with self.assertRaisesRegex(
                    ValueError, "Unsupported business-rule source"
                ):
                    module.source_business_checks({}, "not_a_source")


class Expression:
    """Symbolic Spark expression, not a second implementation of Spark SQL."""

    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return repr(self.value)

    def __getattr__(self, name):
        return lambda *args: Expression((name, self, args))

    def __getitem__(self, key):
        return Expression(("getitem", self, key))

    def __eq__(self, other):
        return Expression(("eq", self, other))

    def __ne__(self, other):
        return Expression(("ne", self, other))

    def __lt__(self, other):
        return Expression(("lt", self, other))

    def __le__(self, other):
        return Expression(("le", self, other))

    def __gt__(self, other):
        return Expression(("gt", self, other))

    def __ge__(self, other):
        return Expression(("ge", self, other))

    def __and__(self, other):
        return Expression(("and", self, other))

    def __or__(self, other):
        return Expression(("or", self, other))

    def __invert__(self):
        return Expression(("not", self))


def canonical(value):
    """Ignore set-induced OR ordering, not fields, operators or null behavior."""
    if isinstance(value, Expression):
        return canonical(value.value)
    if isinstance(value, list):
        return [canonical(item) for item in value]
    if isinstance(value, tuple):
        if len(value) == 3 and value[0] == "or":
            terms = []
            for child in value[1:]:
                child = canonical(child)
                terms.extend(
                    child[1]
                    if isinstance(child, tuple) and child[0] == "or"
                    else [child]
                )
            return ("or", tuple(sorted(terms, key=repr)))
        return tuple(canonical(item) for item in value)
    return value


class Functions:
    def __getattr__(self, name):
        return lambda *args: Expression((name, args))


class Frame:
    """Minimal operation recorder used to compare old and new adapter plans."""

    def __init__(self, columns=(), calls=None):
        self.columns = list(columns)
        self.calls = [] if calls is None else calls
        self.sparkSession = SimpleNamespace(createDataFrame=self.create_frame)

    def create_frame(self, rows, schema):
        self.calls.append(("createDataFrame", rows, schema))
        return Frame(["entity_system_number", "SupplierNumber"], self.calls)

    def withColumn(self, name, value):
        self.calls.append(("withColumn", name, repr(value)))
        return Frame(list(dict.fromkeys([*self.columns, name])), self.calls)

    def join(self, other, *args):
        self.calls.append(("join", other.columns, repr(args)))
        return Frame(list(dict.fromkeys([*self.columns, *other.columns])), self.calls)

    def select(self, *columns):
        self.calls.append(("select", repr(columns)))
        # The old fact code only needs the return's filter/count validation.
        return Frame(self.columns, self.calls)

    def filter(self, condition):
        return self

    def limit(self, number):
        return self

    def count(self):
        return 0

    def drop(self, *columns):
        return self


class GoldAdapterParityTests(unittest.TestCase):
    def test_department_and_age_band_are_branch_exact(self):
        for name in ("_department", "source_age_band"):
            self.assertEqual(
                function_ast(getattr(adapters, name)),
                function_ast(getattr(old_gold, name)),
            )
        with patch("pyspark.sql.functions", Functions()):
            for source, columns in (
                ("bas", ["responsibility_parent_number"]),
                ("bas", []),
                ("persal", ["dept"]),
                ("persal", []),
                ("csd", ["dept"]),
            ):
                left, right = Frame(columns), Frame(columns)
                config = {"department_mapping": [{"code": "HEALTH", "id": "1"}]}
                adapters._department(left, source, config)
                old_gold._department(right, source, config)
                self.assertEqual(left.calls, right.calls)

    def test_alternate_dimension_keys_match_old_lookup_selection(self):
        with patch("pyspark.sql.functions", Functions()):
            for name, keys, columns in (
                ("dim_employee", ["employee_id"], ["persal_num"]),
                ("dim_employee", ["employee_id"], ["employee_id", "persal_num"]),
                ("dim_employee", ["employee_id"], []),
                ("dim_post", ["estab_id"], ["post_num"]),
                ("dim_post", ["estab_id"], ["estab_id", "post_num"]),
                ("dim_post", ["estab_id"], []),
                ("dim_account", ["item"], ["item"]),
            ):
                spec = {"name": name, "keys": keys}
                frame, dimension = Frame(columns), Frame(["dimension_sk"])
                old_gold.attach_dimension(frame, dimension, spec, "test_sk")
                chosen = adapters.dimension_lookup_keys(frame, spec)
                selects = [c[1] for c in dimension.calls if c[0] == "select"]
                if not set(chosen) <= set(columns):
                    self.assertFalse(selects)
                else:
                    for key in chosen:
                        self.assertIn(repr(("col", (key,))), selects[0])

    def test_fact_adapters_match_old_source_mappings_and_date_expressions(self):
        config = {
            "department_mapping": [{"code": "HEALTH", "id": "1"}],
            "supplier_crosswalk": [
                {"entity_system_number": "123", "SupplierNumber": "S1"}
            ],
            "dimensions": [],
            "sources": [
                {"source_code": code, "source_instance": code}
                for code in ("bas", "csd", "persal")
            ],
            "contract_hash": "a" * 64,
        }
        cases = (
            ("bas", ["entity_system_number", "ParentSupplierNumber"], False),
            ("csd", ["ParentSupplierNumber"], False),
            ("csd", ["SupplierNumber", "ParentSupplierNumber"], True),
            ("persal", ["dept"], False),
            ("bas", ["source_to_action_days"], False),
        )
        with (
            patch("pyspark.sql.functions", Functions()),
            patch.object(old_gold, "_latest", side_effect=lambda frame, keys: frame),
            patch.object(old_gold, "_assert_unique"),
            patch.object(old_gold, "_gold_hash", return_value=Expression("hash")),
            patch.object(old_gold, "_gold_sk", return_value=Expression("sk")),
        ):
            for source, fields, snapshot in cases:
                with self.subTest(source=source, fields=fields, snapshot=snapshot):
                    spec = {
                        "name": "fact_parity",
                        "source": source,
                        "table": "table",
                        "snapshot": snapshot,
                        "keys": ["id"],
                        "date_field": "event",
                        "period_field": "",
                        "date_roles": {"role": "timestamp"},
                        "attributes": [],
                        "measures": [],
                    }
                    old_frame = Frame([*fields, "id"])
                    old_gold.build_fact(
                        old_frame, spec, {}, config, date(2026, 11, 2), "release"
                    )
                    new_frame = Frame([*fields, "id"])
                    new_frame = adapters._department(new_frame, source, config)
                    new_frame = adapters.attach_supplier_identity(
                        new_frame, config, assert_unique=lambda *args: None
                    )
                    # Source-specific enrichment before fact projection is exact.
                    self.assertEqual(old_frame.calls[:-1], new_frame.calls)
                    projection = old_frame.calls[-1][1]
                    event = adapters.fact_event_date(new_frame, spec, date(2026, 11, 2))
                    self.assertIn(repr(event), projection)
                    self.assertIn(
                        repr(adapters.supplier_business_key(new_frame)), projection
                    )
                    for value in adapters.fact_date_roles(spec).values():
                        self.assertIn(repr(value), projection)
                    extra_columns = adapters.fact_extra_columns(new_frame, spec)
                    if "source_to_action_days" in new_frame.columns:
                        self.assertEqual(len(extra_columns), 1)
                        self.assertIn(repr(extra_columns[0]), projection)
                    else:
                        self.assertEqual(extra_columns, [])

    def test_fact_extra_columns_match_original_projection_conditional(self):
        original = ast.parse(inspect.getsource(old_gold.build_fact))
        selection = next(
            node
            for node in ast.walk(original)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "select"
        )
        expected = selection.args[-1]
        self.assertIsInstance(expected, ast.Starred)
        extracted = ast.parse(inspect.getsource(adapters.fact_extra_columns))
        actual = next(
            node for node in extracted.body[0].body if isinstance(node, ast.Return)
        )
        self.assertEqual(ast.dump(actual.value), ast.dump(expected.value))
        self.assertIs(
            adapters.RUNTIME_GLOBALS["fact_extra_columns"],
            adapters.fact_extra_columns,
        )

    def test_dimension_role_names_match_old_fact_projection(self):
        tree = ast.parse(inspect.getsource(old_gold.build_fact))
        assignment = next(
            node
            for node in tree.body[0].body
            if isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id == "roles"
                for target in node.targets
            )
        )
        self.assertEqual(adapters.DIMENSION_ROLES, ast.literal_eval(assignment.value))
        for name, output in adapters.DIMENSION_ROLES.items():
            self.assertEqual(adapters.dimension_output_key(name), output)
        self.assertEqual(adapters.dimension_output_key("dim_other"), "dim_other_sk")

    def test_supplier_crosswalk_duplicate_guard_and_missing_date_error(self):
        config = {
            "supplier_crosswalk": [
                {"entity_system_number": "1", "SupplierNumber": "S1"}
            ]
        }
        with patch("pyspark.sql.functions", Functions()):

            def reject(frame, keys, label):
                self.assertEqual(keys, ["entity_system_number"])
                self.assertEqual(label, "Demo supplier crosswalk")
                raise RuntimeError("duplicate key")

            with self.assertRaisesRegex(RuntimeError, "duplicate key"):
                adapters.attach_supplier_identity(
                    Frame(["entity_system_number"]), config, assert_unique=reject
                )
            with self.assertRaisesRegex(
                ValueError, "atomic facts require a source event date"
            ):
                adapters.fact_event_date(
                    Frame(),
                    {"name": "bad", "snapshot": False, "date_field": ""},
                    date(2026, 1, 1),
                )


if __name__ == "__main__":
    unittest.main()
