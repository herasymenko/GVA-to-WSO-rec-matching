from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
import unicodedata

import pandas as pd

from schema_contracts import REFERENCE_SOURCE_FIELDS, REQUIRED_SOURCE_FIELDS, SOURCE_ALIASES, TR_COLUMNS
from schema_errors import SchemaError


def _normalize_header(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = " ".join(text.strip().split())
    return text.casefold()


def _build_alias_index() -> dict[str, str]:
    index: dict[str, str] = {}
    for canonical, aliases in SOURCE_ALIASES.items():
        for alias in aliases:
            index[_normalize_header(alias)] = canonical
    return index


def _discover_rec_files(input_dir: Path) -> list[Path]:
    files = sorted(p for p in input_dir.glob("*.xlsx") if p.is_file())
    if len(files) != 2:
        raise SchemaError(
            code="SCHEMA_REC_FILE_COUNT",
            message=f"Expected exactly 2 rec files in {input_dir}, found {len(files)}",
            hint="Place only gva_rec and wso_rec xlsx files in data/input before running stage=schema.",
        )
    return files


def _resolve_columns(columns: list[str], alias_index: dict[str, str]) -> dict[str, str]:
    resolved: dict[str, str] = {}
    for source_col in columns:
        key = _normalize_header(source_col)
        canonical = alias_index.get(key)
        if canonical and canonical not in resolved:
            resolved[canonical] = source_col
    return resolved


def _detect_rec_type(df: pd.DataFrame, set_id_col: str, file_path: Path) -> str:
    series = df[set_id_col].dropna().astype(str).str.strip().str.upper()
    sample = series.head(250)
    if sample.empty:
        raise SchemaError(
            code="SCHEMA_EMPTY_SET_ID",
            message="Set ID column has no values to detect file role",
            hint="Ensure Set ID has AIS... for WSO and GVA... for GVA records.",
            file_path=str(file_path),
        )
    ais_hits = sample.str.startswith("AIS").sum()
    gva_hits = sample.str.startswith("GVA").sum()
    if ais_hits > gva_hits and ais_hits > 0:
        return "WSO"
    if gva_hits > ais_hits and gva_hits > 0:
        return "GVA"
    raise SchemaError(
        code="SCHEMA_REC_TYPE_AMBIGUOUS",
        message="Could not detect whether rec file is WSO or GVA",
        hint="Expected dominant Set ID prefixes AIS... or GVA...",
        file_path=str(file_path),
    )


@dataclass
class FileSchemaMetrics:
    file_name: str
    rec_type: str
    rows: int
    reference_sources: list[str]
    required_null_counts: dict[str, int]
    resolved_columns: dict[str, str]
    elapsed_ms: int


def _validate_file_schema(file_path: Path, alias_index: dict[str, str]) -> FileSchemaMetrics:
    started = perf_counter()

    header_df = pd.read_excel(file_path, skiprows=1, nrows=0)
    source_columns = [str(c) for c in header_df.columns]
    resolved = _resolve_columns(source_columns, alias_index)

    missing = [
        REQUIRED_SOURCE_FIELDS[key]
        for key in REQUIRED_SOURCE_FIELDS
        if REQUIRED_SOURCE_FIELDS[key] not in resolved
    ]
    if missing:
        raise SchemaError(
            code="SCHEMA_MISSING_COLUMN",
            message=f"Missing required columns: {', '.join(missing)}",
            hint="Check header names and aliases in schema contracts.",
            file_path=str(file_path),
        )

    present_ref_sources = [name for name in REFERENCE_SOURCE_FIELDS if name in resolved]
    if not present_ref_sources:
        raise SchemaError(
            code="SCHEMA_NO_REFERENCE_COLUMNS",
            message="No reference source columns found",
            hint="At least one of Ref1, Ref2, Ref3, Ref4, Original ID, Asset Desc must exist.",
            file_path=str(file_path),
        )

    read_cols = [resolved[name] for name in REQUIRED_SOURCE_FIELDS.values()] + [resolved[n] for n in present_ref_sources]
    # Deduplicate while preserving order.
    read_cols = list(dict.fromkeys(read_cols))
    df = pd.read_excel(file_path, skiprows=1, usecols=read_cols)

    rec_type = _detect_rec_type(df, resolved["Set ID"], file_path)

    null_counts: dict[str, int] = {}
    for canonical_name in REQUIRED_SOURCE_FIELDS.values():
        src = resolved[canonical_name]
        null_counts[canonical_name] = int(df[src].isna().sum())

    elapsed_ms = int((perf_counter() - started) * 1000)
    return FileSchemaMetrics(
        file_name=file_path.name,
        rec_type=rec_type,
        rows=len(df),
        reference_sources=present_ref_sources,
        required_null_counts=null_counts,
        resolved_columns=resolved,
        elapsed_ms=elapsed_ms,
    )


def run_schema_stage(project_root: Path) -> int:
    stage_started = perf_counter()
    input_dir = project_root / "data" / "input"

    if not input_dir.exists():
        raise SchemaError(
            code="SCHEMA_INPUT_DIR_MISSING",
            message=f"Input directory does not exist: {input_dir}",
            hint="Create data/input and place two rec xlsx files there.",
        )

    rec_files = _discover_rec_files(input_dir)
    alias_index = _build_alias_index()

    results: list[FileSchemaMetrics] = []
    for file_path in rec_files:
        results.append(_validate_file_schema(file_path, alias_index))

    rec_types = sorted(r.rec_type for r in results)
    if rec_types != ["GVA", "WSO"]:
        raise SchemaError(
            code="SCHEMA_ROLE_MISMATCH",
            message=f"Expected one GVA and one WSO file, got roles: {rec_types}",
            hint="Verify Set ID prefixes in both rec files.",
        )

    print("[schema] status=ok")
    print(f"[schema] tr_columns={','.join(TR_COLUMNS)}")
    for r in results:
        print(
            "[schema] "
            f"file={r.file_name} "
            f"rec_type={r.rec_type} rows={r.rows} "
            f"ref_sources={','.join(r.reference_sources)} elapsed_ms={r.elapsed_ms}"
        )
        print(
            "[schema] "
            f"file={r.file_name} resolved={{{', '.join(f'{k}:{v}' for k, v in sorted(r.resolved_columns.items()))}}}"
        )
        print(
            "[schema] "
            f"file={r.file_name} required_nulls={{{', '.join(f'{k}:{v}' for k, v in sorted(r.required_null_counts.items()))}}}"
        )

    stage_elapsed_ms = int((perf_counter() - stage_started) * 1000)
    print(f"[schema] total_elapsed_ms={stage_elapsed_ms}")
    return 0
