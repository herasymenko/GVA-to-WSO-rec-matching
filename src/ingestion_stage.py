from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

from schema_errors import SchemaError


@dataclass
class IngestionResult:
    rec_files: list[Path]
    mapping_file: Path
    key_values_file: Path
    elapsed_ms: int


def _xlsx_files(folder: Path) -> list[Path]:
    return sorted(p for p in folder.glob("*.xlsx") if p.is_file())


def _find_single_by_tokens(files: list[Path], tokens: tuple[str, ...], code: str, kind: str) -> Path:
    matches = [p for p in files if any(token in p.stem.casefold() for token in tokens)]
    if not matches:
        raise SchemaError(
            code=code,
            message=f"{kind} file was not found in data/static",
            hint=f"Expected file name containing one of: {', '.join(tokens)}",
        )
    if len(matches) > 1:
        raise SchemaError(
            code=f"{code}_AMBIGUOUS",
            message=f"Multiple candidates found for {kind}: {', '.join(p.name for p in matches)}",
            hint="Keep exactly one file per static role.",
        )
    return matches[0]


def discover_inputs(project_root: Path) -> IngestionResult:
    started = perf_counter()
    input_dir = project_root / "data" / "input"
    static_dir = project_root / "data" / "static"

    if not input_dir.exists():
        raise SchemaError(
            code="INGESTION_INPUT_DIR_MISSING",
            message=f"Input directory does not exist: {input_dir}",
            hint="Create data/input and place rec xlsx files there.",
        )
    if not static_dir.exists():
        raise SchemaError(
            code="INGESTION_STATIC_DIR_MISSING",
            message=f"Static directory does not exist: {static_dir}",
            hint="Create data/static and place mapping/key files there.",
        )

    rec_files = _xlsx_files(input_dir)
    if len(rec_files) != 2:
        raise SchemaError(
            code="INGESTION_REC_FILE_COUNT",
            message=f"Expected exactly 2 rec files in data/input, found {len(rec_files)}",
            hint="Keep only two rec xlsx files in data/input.",
        )

    static_files = _xlsx_files(static_dir)
    if not static_files:
        raise SchemaError(
            code="INGESTION_NO_STATIC_FILES",
            message="No .xlsx files found in data/static",
            hint="Add mapping and key-values files.",
        )

    mapping_file = _find_single_by_tokens(
        static_files,
        ("mapping",),
        "INGESTION_MAPPING_NOT_FOUND",
        "mapping",
    )
    key_values_file = _find_single_by_tokens(
        static_files,
        ("key", "keys", "key_values", "key-values"),
        "INGESTION_KEYS_NOT_FOUND",
        "key-values",
    )

    if mapping_file == key_values_file:
        raise SchemaError(
            code="INGESTION_STATIC_ROLE_CONFLICT",
            message=f"Single file matched multiple static roles: {mapping_file.name}",
            hint="Use distinct names for mapping and key-values files.",
        )

    used = {mapping_file, key_values_file}
    unexpected = [p.name for p in static_files if p not in used]
    if unexpected:
        raise SchemaError(
            code="INGESTION_UNEXPECTED_STATIC_FILES",
            message=f"Unexpected static files: {', '.join(unexpected)}",
            hint="Keep only mapping and key-values xlsx files in data/static.",
        )

    for rec in rec_files:
        if rec.stat().st_size == 0:
            raise SchemaError(
                code="INGESTION_EMPTY_REC_FILE",
                message=f"Rec file is empty: {rec.name}",
                hint="Regenerate or replace the empty file.",
            )
    for static_file in used:
        if static_file.stat().st_size == 0:
            raise SchemaError(
                code="INGESTION_EMPTY_STATIC_FILE",
                message=f"Static file is empty: {static_file.name}",
                hint="Regenerate or replace the empty file.",
            )

    result = IngestionResult(
        rec_files=rec_files,
        mapping_file=mapping_file,
        key_values_file=key_values_file,
        elapsed_ms=int((perf_counter() - started) * 1000),
    )
    return result


def run_ingestion_stage(project_root: Path) -> int:
    result = discover_inputs(project_root)

    print("[ingestion] status=ok")
    print(f"[ingestion] rec_files={','.join(p.name for p in result.rec_files)}")
    print(f"[ingestion] mapping_file={result.mapping_file.name}")
    print(f"[ingestion] key_values_file={result.key_values_file.name}")
    print(f"[ingestion] total_elapsed_ms={result.elapsed_ms}")
    return 0
