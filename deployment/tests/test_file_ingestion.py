"""Transport-neutral arrival sealing, file membership and distinct Copy adapters."""

from __future__ import annotations

import json
import unittest
from pathlib import Path

import test_run_manifests as manifests
from fabric_medallion_deployer import definitions

ROOT = Path(__file__).resolve().parents[2]


class FileArrivalTests(manifests.ManifestFixture):
    def observations(self, **changes):
        row = {
            "relative_path": "postings_01.csv",
            "size_bytes": 100,
            "modified_utc": "2026-11-06T08:00:00Z",
        }
        row.update(changes)
        return [row]

    def test_marker_policy_requires_the_explicit_completion_signal(self):
        policy = {"kind": "marker", "marker": "_SUCCESS"}
        with self.assertRaisesRegex(RuntimeError, "completion|marker"):
            self.runtime.seal_file_arrival(
                self.observations(),
                policy,
                observed_utc="2026-11-06T09:00:00Z",
            )
        sealed = self.runtime.seal_file_arrival(
            self.observations(),
            policy,
            observed_utc="2026-11-06T09:00:00Z",
            present_markers=["_SUCCESS"],
        )
        self.assertEqual(len(sealed.files), 1)
        self.assertEqual(len(sealed.membership_sha256), 64)

    def test_stability_policy_requires_two_identical_aged_observations(self):
        policy = {"kind": "stable", "minimum_age_seconds": 60, "stable_for_seconds": 60}
        now = "2026-11-06T09:00:00Z"
        for previous, previous_time in (
            (None, None),
            (self.observations(size_bytes=99), "2026-11-06T08:58:00Z"),
            (self.observations(), "2026-11-06T08:59:30Z"),
        ):
            with (
                self.subTest(previous_time=previous_time),
                self.assertRaises(RuntimeError),
            ):
                self.runtime.seal_file_arrival(
                    self.observations(),
                    policy,
                    observed_utc=now,
                    previous_files=previous,
                    previous_observed_utc=previous_time,
                )
        result = self.runtime.seal_file_arrival(
            self.observations(),
            policy,
            observed_utc=now,
            previous_files=self.observations(),
            previous_observed_utc="2026-11-06T08:58:00Z",
        )
        self.assertEqual(result.files[0].relative_path, "postings_01.csv")

    def test_membership_is_order_independent_but_exact_and_cannot_escape(self):
        records = self.observations() + self.observations(
            relative_path="part/postings_02.csv"
        )
        policy = {"kind": "marker", "marker": "_SUCCESS"}
        options = {
            "observed_utc": "2026-11-06T09:00:00Z",
            "present_markers": ["_SUCCESS"],
        }
        first = self.runtime.seal_file_arrival(records, policy, **options)
        second = self.runtime.seal_file_arrival(
            list(reversed(records)), policy, **options
        )
        self.assertEqual(first.membership_sha256, second.membership_sha256)
        for change in (
            self.observations(relative_path="../other.csv"),
            self.observations(relative_path="https://example.invalid/file"),
            self.observations(relative_path="part\\other.csv"),
            self.observations(size_bytes=True),
            self.observations(size_bytes=-1),
            records + records,
        ):
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                self.runtime.seal_file_arrival(change, policy, **options)

    def test_empty_files_and_future_modified_times_are_not_silently_accepted(self):
        policy = {"kind": "marker", "marker": "_SUCCESS"}
        options = {
            "observed_utc": "2026-11-06T09:00:00Z",
            "present_markers": ["_SUCCESS"],
        }
        for files in (
            [],
            self.observations(size_bytes=0),
            self.observations(modified_utc="2026-11-07T08:00:00Z"),
        ):
            with self.subTest(files=files), self.assertRaises(RuntimeError):
                self.runtime.seal_file_arrival(files, policy, **options)
        result = self.runtime.seal_file_arrival(
            self.observations(size_bytes=0), policy, allow_empty_files=True, **options
        )
        self.assertEqual(result.files[0].size_bytes, 0)

    def test_changed_landed_membership_blocks_processing(self):
        policy = {"kind": "marker", "marker": "_SUCCESS"}
        sealed = self.runtime.seal_file_arrival(
            self.observations(),
            policy,
            observed_utc="2026-11-06T09:00:00Z",
            present_markers=["_SUCCESS"],
        )
        self.runtime.verify_file_membership(sealed, self.observations())
        with self.assertRaisesRegex(RuntimeError, "membership|changed"):
            self.runtime.verify_file_membership(
                sealed, self.observations(size_bytes=101)
            )

    def test_stability_evidence_survives_serialization_and_revalidation(self):
        policy = {"kind": "stable", "minimum_age_seconds": 60, "stable_for_seconds": 60}
        sealed = self.runtime.seal_file_arrival(
            self.observations(),
            policy,
            observed_utc="2026-11-06T09:00:00Z",
            previous_files=self.observations(),
            previous_observed_utc="2026-11-06T08:58:00Z",
        )
        evidence = json.loads(sealed.evidence_json)
        restored = self.runtime.seal_file_arrival(
            self.observations(), policy, observed_utc=sealed.observed_utc, **evidence
        )
        self.assertEqual(restored, sealed)
        evidence["previous_files"] = self.observations(size_bytes=99)
        with self.assertRaisesRegex(RuntimeError, "stable"):
            self.runtime.seal_file_arrival(
                self.observations(),
                policy,
                observed_utc=sealed.observed_utc,
                **evidence,
            )

    def test_invalid_or_secret_policy_fields_fail_explicitly(self):
        for policy in (
            {"kind": "exists"},
            {"kind": "stable", "minimum_age_seconds": 0, "stable_for_seconds": 0},
            {"kind": "marker", "marker": "../_SUCCESS"},
            {
                "kind": "marker",
                "marker": "_SUCCESS",
                "password": "synthetic_rejection_fixture",
            },
        ):
            with self.subTest(policy=policy), self.assertRaises(RuntimeError):
                self.runtime.seal_file_arrival(
                    self.observations(),
                    policy,
                    observed_utc="2026-11-06T09:00:00Z",
                    present_markers=["_SUCCESS"],
                )

    def test_marker_membership_is_not_a_substring_and_previous_time_is_consistent(self):
        with self.assertRaises(RuntimeError):
            self.runtime.seal_file_arrival(
                self.observations(),
                {"kind": "marker", "marker": "_SUCCESS"},
                observed_utc="2026-11-06T09:00:00Z",
                present_markers="not_SUCCESS_a_marker_list",
            )
        with self.assertRaisesRegex(RuntimeError, "previous|observation"):
            self.runtime.seal_file_arrival(
                self.observations(modified_utc="2026-11-06T08:59:00Z"),
                {"kind": "stable", "minimum_age_seconds": 60, "stable_for_seconds": 60},
                observed_utc="2026-11-06T09:00:00Z",
                previous_files=self.observations(modified_utc="2026-11-06T08:59:00Z"),
                previous_observed_utc="2026-11-06T08:58:00Z",
            )


class FileAdapterTests(unittest.TestCase):
    def test_ftp_and_sftp_are_distinct_binary_preserving_adapters(self):
        for kind, setting, location in (
            ("ftp", "FtpReadSettings", "FtpServerLocation"),
            ("sftp", "SftpReadSettings", "SftpLocation"),
        ):
            activity = definitions.file_copy_adapter(ROOT, kind)
            self.assertEqual(activity["type"], "Copy")
            source = activity["typeProperties"]["source"]
            self.assertEqual(source["type"], "BinarySource")
            self.assertEqual(source["storeSettings"]["type"], setting)
            self.assertEqual(
                source["datasetSettings"]["typeProperties"]["location"]["type"],
                location,
            )
            self.assertFalse(source["storeSettings"]["deleteFilesAfterCompletion"])
            self.assertEqual(activity["policy"]["retry"], 0)
            self.assertTrue(activity["policy"]["secureInput"])
            self.assertTrue(activity["policy"]["secureOutput"])
            self.assertEqual(
                activity["typeProperties"]["sink"]["storeSettings"]["copyBehavior"],
                "PreserveHierarchy",
            )
            second = definitions.file_copy_adapter(ROOT, kind)
            activity["typeProperties"].clear()
            self.assertTrue(second["typeProperties"])
        with self.assertRaises(ValueError):
            definitions.file_copy_adapter(ROOT, "file")


if __name__ == "__main__":
    unittest.main()
