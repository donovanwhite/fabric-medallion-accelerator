#!/usr/bin/env python3
"""Generate or check reusable notebooks and the metadata deployment inventory."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import nbformat

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "deployment" / "src"))

from fabric_medallion_deployer.deployer import inventory_manifest
from fabric_medallion_deployer.gold import gold_notebook_document
from fabric_medallion_deployer.orchestrator import (
    bootstrap_configuration,
    metadata_notebook_document,
)
from fabric_medallion_deployer.use_cases import load_use_case


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--use-case")
    parser.add_argument(
        "--metadata",
        action="store_true",
        help="Compatibility flag; metadata generation is now the default",
    )
    args = parser.parse_args()
    try:
        profile = load_use_case(ROOT, args.use_case)
        # Validate selected metadata before touching generated outputs.
        bootstrap_configuration(ROOT, use_case=profile.case_id)
    except (ValueError, OSError) as error:
        print(str(error), file=sys.stderr)
        return 2
    documents = {
        profile.generated / "notebooks" / f"{name}.ipynb": metadata_notebook_document(
            ROOT, step, profile.case_id
        )
        for step, name in profile.notebook_names().items()
    }
    documents[profile.generated / "notebooks" / f"{profile.mlv_notebook}.ipynb"] = (
        gold_notebook_document(ROOT, (), "mlvs", profile.case_id)
    )
    stale = []
    for path, document in documents.items():
        notebook = nbformat.from_dict(document)
        nbformat.validate(notebook)
        if args.check:
            if (
                not path.is_file()
                or json.loads(path.read_text(encoding="utf-8")) != document
            ):
                stale.append(str(path.relative_to(ROOT)))
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                nbformat.writes(notebook).rstrip("\n") + "\n",
                encoding="utf-8",
                newline="\n",
            )
    path = profile.generated / "deployment" / "manifest.json"
    expected = inventory_manifest(profile=profile)
    if args.check:
        if (
            not path.is_file()
            or json.loads(path.read_text(encoding="utf-8")) != expected
        ):
            stale.append(str(path.relative_to(ROOT)))
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(expected, indent=2) + "\n", encoding="utf-8", newline="\n"
        )
    if stale:
        print("Stale generated artifacts:\n" + "\n".join(stale), file=sys.stderr)
        return 1
    print(
        "Checked" if args.check else "Generated",
        f"{len(documents)} reusable notebooks and the {len(expected['items'])}-item manifest",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
