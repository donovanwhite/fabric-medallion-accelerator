"""Immutable source binding, lifecycle payloads and preserved contract identity."""

import base64
import json
import unittest
import uuid
from pathlib import Path

from fabric_medallion_deployer.configuration import (
    bind_configuration,
    lifecycle_pipeline_definition,
)
from fabric_medallion_deployer.metadata import MetadataError, compile_run_plan
from fabric_medallion_deployer.orchestrator import bootstrap_configuration

ROOT = Path(__file__).resolve().parents[2]


class ConfigurationBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.original = bootstrap_configuration(ROOT, use_case="treasury")
        cls.bindings = {
            "workspace_id": str(uuid.UUID(int=101)),
            "source_database_ids": {
                code: str(uuid.UUID(int=i))
                for i, code in enumerate(("bas", "csd", "persal"), 201)
            },
            "connection_ids": {
                code: str(uuid.UUID(int=i))
                for i, code in enumerate(("bas", "csd", "persal"), 301)
            },
        }

    def test_binding_preserves_business_contracts_and_does_not_claim_capability(self):
        bound = bind_configuration(self.original, **self.bindings)
        self.assertNotEqual(bound.version_id, self.original.version_id)
        self.assertEqual(bound, bind_configuration(self.original, **self.bindings))
        for before, after in zip(self.original.feeds, bound.feeds, strict=True):
            self.assertEqual(before.legacy_contract_hash, after.legacy_contract_hash)
            self.assertEqual(before.legacy_bronze_root, after.legacy_bronze_root)
            self.assertIsNone(before.geometry)
            self.assertEqual(
                after.geometry.artifact_id,
                self.bindings["source_database_ids"][after.source_code],
            )
        self.assertTrue(
            all(not capability.verified for capability in bound.capabilities)
        )
        preview = compile_run_plan(bound, "local-preview", "2026-11-09T08:00:00Z")
        self.assertFalse(
            any(
                "Unbound connection" in reason or "geometry" in reason
                for reason in preview.blockers
            )
        )
        self.assertFalse(preview.can_execute)
        self.assertEqual(
            self.original.to_document()["extensions"]["hash_policies"],
            bound.to_document()["extensions"]["hash_policies"],
        )

    def test_binding_rejects_missing_duplicate_or_display_name_identities(self):
        for values in (
            self.bindings | {"workspace_id": "workspace"},
            self.bindings | {"source_database_ids": {}},
            self.bindings
            | {
                "connection_ids": dict.fromkeys(
                    ("bas", "csd", "persal"), str(uuid.UUID(int=1))
                )
            },
        ):
            with self.subTest(values=values), self.assertRaises(MetadataError):
                bind_configuration(self.original, **values)

    def definition(self, operation, **kwargs):
        config = bind_configuration(self.original, **self.bindings)
        definition = lifecycle_pipeline_definition(
            config,
            workspace_id=self.bindings["workspace_id"],
            control_database_id=str(uuid.UUID(int=501)),
            control_connection_id=str(uuid.UUID(int=502)),
            operation=operation,
            **kwargs,
        )
        return config, json.loads(base64.b64decode(definition["parts"][0]["payload"]))

    def test_registration_uses_bound_parameters_and_no_activation(self):
        config, definition = self.definition("register")
        script = definition["properties"]["activities"][0]
        self.assertEqual(script["policy"]["retry"], 0)
        self.assertTrue(script["policy"]["secureOutput"])
        query = script["typeProperties"]["scripts"][0]
        self.assertEqual(
            query["text"],
            "EXEC cfg.usp_register_version @version_id,@definition_json,@definition_sha256;",
        )
        self.assertEqual(
            next(
                p["value"]
                for p in query["parameters"]
                if p["name"] == "definition_json"
            ),
            config.definition_json,
        )
        self.assertNotIn("activate_version", json.dumps(definition))

    def test_activation_and_rollback_require_explicit_approval_and_CAS(self):
        for operation in ("approve", "activate", "rollback"):
            with self.subTest(operation=operation), self.assertRaises(MetadataError):
                self.definition(operation)
        for operation in ("activate", "rollback"):
            _, definition = self.definition(
                operation,
                activation_approval="approved-local-export",
                expected_active_version="previous-version",
            )
            query = definition["properties"]["activities"][0]["typeProperties"][
                "scripts"
            ][0]
            self.assertIn("cfg.usp_activate_version", query["text"])
            self.assertEqual(query["parameters"][-1]["value"], "previous-version")
        with self.assertRaises(MetadataError):
            self.definition(
                "validate",
                validation_evidence={"passed": True},
                validator_sha256="a" * 64,
            )
