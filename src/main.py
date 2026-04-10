from __future__ import annotations

import argparse
import sys
from pathlib import Path

from schema_errors import SchemaError
from schema_stage import run_schema_stage


def _build_parser() -> argparse.ArgumentParser:
	parser = argparse.ArgumentParser(description="GVA <-> WSO reconciliation pipeline")
	parser.add_argument(
		"--stage",
		default="schema",
		choices=["schema"],
		help="Pipeline stage to run",
	)
	parser.add_argument(
		"--project-root",
		default=str(Path(__file__).resolve().parents[1]),
		help="Project root containing data/ and src/",
	)
	return parser


def main() -> int:
	parser = _build_parser()
	args = parser.parse_args()

	project_root = Path(args.project_root).resolve()

	try:
		if args.stage == "schema":
			return run_schema_stage(project_root)
		raise SchemaError(
			code="MAIN_UNKNOWN_STAGE",
			message=f"Unsupported stage: {args.stage}",
			hint="Use --stage schema",
		)
	except SchemaError as exc:
		print(str(exc), file=sys.stderr)
		return 2


if __name__ == "__main__":
	raise SystemExit(main())
