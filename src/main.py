from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pipeline_stage import run_pipeline_stage
from schema_errors import SchemaError


def _parse_project_root() -> Path:
	parser = argparse.ArgumentParser(description="GVA <-> WSO reconciliation pipeline")
	parser.add_argument(
		"--project-root",
		default=str(Path(__file__).resolve().parents[1]),
		help="Project root containing data/ and src/",
	)
	args = parser.parse_args()
	return Path(args.project_root).resolve()


def main() -> int:
	project_root = _parse_project_root()

	try:
		return run_pipeline_stage(project_root)
	except SchemaError as exc:
		print(str(exc), file=sys.stderr)
		return 2


if __name__ == "__main__":
	raise SystemExit(main())
