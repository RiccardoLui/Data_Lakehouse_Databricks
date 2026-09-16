from __future__ import annotations

import ast
import json
import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
ERRORS: list[str] = []


def error(message: str) -> None:
    ERRORS.append(message)
    print(f"ERROR: {message}")


def validate_notebooks() -> None:
    for path in ROOT.rglob("*.ipynb"):
        try:
            with path.open("r", encoding="utf-8") as handle:
                notebook = json.load(handle)
            if "cells" not in notebook:
                error(f"Notebook has no cells: {path.relative_to(ROOT)}")
        except Exception as exc:
            error(f"Invalid notebook JSON {path.relative_to(ROOT)}: {exc}")


def validate_python() -> None:
    for path in ROOT.rglob("*.py"):
        if ".git" in path.parts:
            continue
        try:
            ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError as exc:
            error(f"Python syntax error in {path.relative_to(ROOT)}: {exc}")


def validate_yaml() -> None:
    for pattern in ("*.yml", "*.yaml"):
        for path in ROOT.rglob(pattern):
            try:
                yaml.safe_load(path.read_text(encoding="utf-8"))
            except Exception as exc:
                error(f"Invalid YAML {path.relative_to(ROOT)}: {exc}")


def validate_bundle_notebook_paths() -> None:
    resources_dir = ROOT / "resources"
    for path in resources_dir.glob("*.yml"):
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        jobs = payload.get("resources", {}).get("jobs", {})
        for job_name, job in jobs.items():
            for task in job.get("tasks", []):
                notebook_path = task.get("notebook_task", {}).get("notebook_path")
                if not notebook_path:
                    continue
                resolved = (path.parent / notebook_path).resolve()
                if not resolved.exists():
                    error(
                        f"Bundle job {job_name}/{task.get('task_key')} references missing file: "
                        f"{notebook_path}"
                    )


def scan_obvious_secrets() -> None:
    patterns = {
        "Databricks PAT": re.compile(r"\bdapi[a-zA-Z0-9]{20,}\b"),
        "AWS access key": re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
        "Private key": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    }
    text_extensions = {".py", ".sql", ".yml", ".yaml", ".md", ".json", ".txt"}
    for path in ROOT.rglob("*"):
        if not path.is_file() or path.suffix.lower() not in text_extensions:
            continue
        if ".git" in path.parts:
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        for label, regex in patterns.items():
            if regex.search(text):
                error(f"Possible {label} committed in {path.relative_to(ROOT)}")


def validate_required_files() -> None:
    required = [
        ROOT / "databricks.yml",
        ROOT / "resources" / "medallion_jobs.yml",
        ROOT / "Governance" / "governance_setup.py",
    ]
    for path in required:
        if not path.exists():
            error(f"Missing required project file: {path.relative_to(ROOT)}")


def main() -> int:
    validate_required_files()
    validate_notebooks()
    validate_python()
    validate_yaml()
    validate_bundle_notebook_paths()
    scan_obvious_secrets()

    if ERRORS:
        print(f"\nValidation failed with {len(ERRORS)} error(s).")
        return 1

    print("\nRepository validation passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
