"""Versioned rule dispatch with complete baseline checks and no executable metadata."""

from __future__ import annotations

import copy
import importlib.util
import json
import unittest
from pathlib import Path

from fabric_medallion_deployer.orchestrator import bootstrap_configuration

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "treasury_rule_contracts", ROOT / "artefacts" / "medallion" / "rules.py"
)
rules = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(rules)


class MetadataRuleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = bootstrap_configuration(ROOT)

    def profile(self):
        obj = next(obj for obj in self.config.objects if obj.layer == "silver")
        return [
            {
                "id": rule.id,
                "handler": rule.handler,
                "version": rule.version,
                "severity": rule.severity,
                "action": rule.action,
                "parameters": json.loads(rule.parameters_json),
            }
            for rule in self.config.rules
            if rule.object_id == obj.id
            and rule.handler != "legacy.source_business_checks"
        ]

    def test_every_seeded_object_and_source_rule_has_a_supported_version(self):
        for obj in self.config.objects:
            if obj.layer != "silver":
                continue
            profile = [
                {
                    "id": rule.id,
                    "handler": rule.handler,
                    "version": rule.version,
                    "severity": rule.severity,
                    "action": rule.action,
                    "parameters": json.loads(rule.parameters_json),
                }
                for rule in self.config.rules
                if rule.object_id == obj.id
                and rule.handler != "legacy.source_business_checks"
            ]
            checked = rules.validate_rule_specs(profile, scope="object")
            self.assertEqual(len(checked), 3)

    def test_missing_unknown_duplicate_or_unversioned_handlers_fail_before_processing(
        self,
    ):
        for transform in (
            lambda profile: profile.pop(),
            lambda profile: profile[0].update(handler="eval_python"),
            lambda profile: profile[0].update(version="latest"),
            lambda profile: profile.append(copy.deepcopy(profile[0])),
        ):
            profile = self.profile()
            transform(profile)
            with self.subTest(transform=transform), self.assertRaises(ValueError):
                rules.validate_rule_specs(profile, scope="object")

    def test_baseline_checks_cannot_be_silently_demoted_to_warnings(self):
        profile = self.profile()
        profile[0].update(severity="warning", action="warn")
        with self.assertRaisesRegex(ValueError, "baseline|blocking"):
            rules.validate_rule_specs(profile, scope="object")

    def test_generic_checks_have_typed_parameters_and_actionable_versions(self):
        profile = self.profile()
        generic = {
            "id": "amount-nonnegative",
            "handler": "value_range",
            "version": "1",
            "severity": "warning",
            "action": "warn",
            "parameters": {"column": "amount", "minimum": 0, "maximum": 100},
        }
        checked = rules.validate_rule_specs(profile + [generic], scope="object")
        self.assertEqual(checked[-1]["handler"], "value_range")
        for changes in (
            {"parameters": {"column": "amount;DELETE", "minimum": 0}},
            {"parameters": {"column": "amount", "minimum": 10, "maximum": 0}},
            {"parameters": {"column": "amount", "expression": "exec('bad')"}},
            {"action": "drop_silently"},
        ):
            invalid = {**generic, **changes}
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                rules.validate_rule_specs(profile + [invalid], scope="object")


if __name__ == "__main__":
    unittest.main()
