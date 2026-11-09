"""Typed configuration validation without Spark, credentials or cloud calls."""

from __future__ import annotations

import unittest

from fabric_medallion_deployer.metadata import (
    MetadataError,
    load_configuration,
    metadata_digest,
)
from metadata_fixtures import metadata_document


class MetadataTests(unittest.TestCase):
    def test_roundtrip_is_canonical_and_not_mutable_by_the_caller(self) -> None:
        document = metadata_document()
        config = load_configuration(document)
        expected = config.definition_sha256
        document["feeds"][0]["description"] = "changed"
        self.assertEqual(config.definition_sha256, expected)
        self.assertNotEqual(metadata_digest(document), expected)
        restored = config.to_document()
        restored["objects"].clear()
        self.assertEqual(len(config.objects), 2)
        self.assertEqual(metadata_digest(config.to_document()), expected)
        self.assertEqual(config.feeds[0].bronze_prefix, "Files/bronze/bas_finance_sql")

    def test_sql_geometry_is_explicit_and_never_derived_from_a_display_name(
        self,
    ) -> None:
        document = metadata_document()
        document["feeds"][0]["geometry"] = {
            "workspace_id": "44444444-4444-4444-8444-444444444444",
            "artifact_id": "55555555-5555-4555-8555-555555555555",
        }
        config = load_configuration(document)
        self.assertEqual(
            config.feeds[0].geometry.artifact_id,
            "55555555-5555-4555-8555-555555555555",
        )
        document["feeds"][0]["geometry"]["artifact_id"] = "db_bas_finance"
        with self.assertRaisesRegex(MetadataError, "UUID|geometry"):
            load_configuration(document)

    def test_sql_projection_cannot_inject_expressions_or_source_identifiers(
        self,
    ) -> None:
        for field, value in (
            ("projection_sql", "* FROM bas.postings; DELETE FROM bas.postings;--"),
            ("source_schema", "bas].[other"),
            ("source_object", "postings;SELECT 1"),
        ):
            document = metadata_document()
            document["objects"][0]["contract"][field] = value
            with self.subTest(field=field), self.assertRaises(MetadataError):
                load_configuration(document)

    def test_projection_reordering_does_not_change_the_hash_input_order(self) -> None:
        document = metadata_document()
        document["objects"][1]["columns"].reverse()
        config = load_configuration(document)
        policy = config.to_document()["extensions"]["hash_policies"][
            "bas-silver-postings"
        ]
        self.assertEqual(policy["value_columns"], ["id", "amount"])
        self.assertEqual([c.name for c in config.objects[1].columns], ["amount", "id"])

    def test_unknown_fields_and_credentials_fail_without_revealing_the_value(
        self,
    ) -> None:
        for field in ("unknown", "password", "access_token", "refresh_token"):
            document = metadata_document()
            document["connections"][0][field] = "MUST_NOT_APPEAR_IN_ERROR"
            with self.subTest(field=field), self.assertRaises(MetadataError) as caught:
                load_configuration(document)
            self.assertNotIn("MUST_NOT_APPEAR_IN_ERROR", str(caught.exception))

    def test_duplicate_identity_and_column_names_are_rejected(self) -> None:
        document = metadata_document()
        document["objects"].append(document["objects"][0].copy())
        with self.assertRaisesRegex(MetadataError, "Duplicate"):
            load_configuration(document)
        document = metadata_document()
        document["objects"][0]["columns"][1]["name"] = "ID"
        with self.assertRaisesRegex(MetadataError, "column"):
            load_configuration(document)

    def test_only_one_layout_flag_and_only_its_columns_are_allowed(self) -> None:
        for strategy in ("partitioned", "liquid"):
            document = metadata_document()
            document["objects"][1]["existing_target"] = False
            layout = {
                "layout_strategy": strategy,
                "partition_columns": ["id"] if strategy == "partitioned" else [],
                "cluster_by_columns": ["id"] if strategy == "liquid" else [],
            }
            document["objects"][1]["layout"] = layout
            self.assertEqual(
                load_configuration(document).objects[1].layout.strategy, strategy
            )
            layout["partition_columns"] = ["id"]
            layout["cluster_by_columns"] = ["id"]
            with self.assertRaisesRegex(MetadataError, "inactive"):
                load_configuration(document)

    def test_new_delta_target_requires_layout_but_existing_is_not_rewritten(
        self,
    ) -> None:
        document = metadata_document()
        load_configuration(document)
        document["objects"][1]["existing_target"] = False
        with self.assertRaisesRegex(MetadataError, "layout"):
            load_configuration(document)

    def test_raw_files_do_not_accept_delta_layout(self) -> None:
        document = metadata_document()
        document["objects"][0]["layout"] = {
            "layout_strategy": "liquid",
            "partition_columns": [],
            "cluster_by_columns": ["id"],
        }
        with self.assertRaisesRegex(MetadataError, "files"):
            load_configuration(document)

    def test_missing_references_and_cycles_are_rejected(self) -> None:
        document = metadata_document()
        document["dependencies"][0]["upstream_id"] = "missing"
        with self.assertRaisesRegex(MetadataError, "dependency"):
            load_configuration(document)
        document = metadata_document()
        document["dependencies"].append(
            {
                "upstream_id": "bas-silver-postings",
                "downstream_id": "bas-bronze-postings",
                "condition": "succeeded",
            }
        )
        with self.assertRaisesRegex(MetadataError, "cycle|layer"):
            load_configuration(document)

    def test_authority_and_target_collisions_are_rejected(self) -> None:
        document = metadata_document()
        second = document["feeds"][0].copy()
        second.update(id="other-feed", source_type="ftp", geometry=None)
        document["feeds"].append(second)
        with self.assertRaisesRegex(MetadataError, "authority|connector"):
            load_configuration(document)
        document = metadata_document()
        second = document["objects"][1].copy()
        second["id"] = "other-object"
        document["objects"].append(second)
        with self.assertRaisesRegex(MetadataError, "target"):
            load_configuration(document)

    def test_keys_ordinals_types_and_boolean_flags_are_validated(self) -> None:
        for change in (
            {"business_keys": ["missing"]},
            {"enabled": "true"},
            {"execution_priority": True},
            {"materialization": "history_and_current"},
        ):
            document = metadata_document()
            document["objects"][1].update(change)
            with self.subTest(change=change), self.assertRaises(MetadataError):
                load_configuration(document)
        document = metadata_document()
        document["objects"][1]["columns"][1]["scale"] = 20
        with self.assertRaisesRegex(MetadataError, "precision|scale"):
            load_configuration(document)

    def test_unverified_capability_is_not_claimed_and_verified_requires_evidence(
        self,
    ) -> None:
        config = load_configuration(metadata_document())
        self.assertFalse(config.capabilities[0].verified)
        document = metadata_document()
        document["capabilities"][0]["verified"] = True
        with self.assertRaisesRegex(MetadataError, "evidence"):
            load_configuration(document)

    def test_rule_actions_and_worker_versions_are_explicit(self) -> None:
        document = metadata_document()
        document["rules"][0]["action"] = "drop_silently"
        with self.assertRaisesRegex(MetadataError, "action"):
            load_configuration(document)
        document = metadata_document()
        document["workers"][0]["code_sha256"] = "latest"
        with self.assertRaisesRegex(MetadataError, "SHA-256"):
            load_configuration(document)

    def test_nonfinite_or_non_json_metadata_is_rejected(self) -> None:
        for value in (float("nan"), float("inf"), object()):
            document = metadata_document()
            document["extensions"]["value"] = value
            with self.subTest(value=type(value)), self.assertRaises(MetadataError):
                load_configuration(document)

    def test_constraints_distinguish_logical_checks_from_engine_enforcement(
        self,
    ) -> None:
        document = metadata_document()
        document["constraints"] = [
            {
                "id": "postings-unique",
                "object_id": "bas-silver-postings",
                "kind": "unique",
                "enforcement": "runtime",
                "definition": {"columns": ["id"]},
            }
        ]
        config = load_configuration(document)
        self.assertEqual(config.constraints[0].enforcement, "runtime")
        document["constraints"][0]["enforcement"] = "implied"
        with self.assertRaisesRegex(MetadataError, "enforcement"):
            load_configuration(document)

    def test_malformed_enum_values_are_configuration_errors_not_type_errors(
        self,
    ) -> None:
        for field in ("materialization", "layer"):
            document = metadata_document()
            document["objects"][0][field] = []
            with self.subTest(field=field), self.assertRaises(MetadataError):
                load_configuration(document)


if __name__ == "__main__":
    unittest.main()
