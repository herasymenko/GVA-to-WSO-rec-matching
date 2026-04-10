from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
import shutil
from time import perf_counter
import unicodedata

import pandas as pd

from ingestion_stage import discover_inputs
from schema_contracts import REFERENCE_SOURCE_FIELDS, REQUIRED_SOURCE_FIELDS, SOURCE_ALIASES, TR_COLUMNS
from schema_errors import SchemaError


SIDE_MAP = {
    "buy": "B",
    "b": "B",
    "sell": "S",
    "s": "S",
    "debit": "D",
    "d": "D",
    "credit": "C",
    "c": "C",
    "long": "L",
    "l": "L",
    "short": "SH",
    "sh": "SH",
}


@dataclass
class CanonicalRunMetrics:
    rows_total: int
    rows_gva: int
    rows_wso: int
    amount_parse_errors: int
    date_parse_errors: int
    side_unknown_count: int
    output_file_name: str
    elapsed_ms: int


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
            code="CANONICAL_EMPTY_SET_ID",
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
        code="CANONICAL_REC_TYPE_AMBIGUOUS",
        message="Could not detect whether rec file is WSO or GVA",
        hint="Expected dominant Set ID prefixes AIS... or GVA...",
        file_path=str(file_path),
    )


def _parse_amount_to_cents(value: object) -> int | None:
    if pd.isna(value):
        return None
    text = str(value).strip().replace(" ", "").replace("\u00a0", "")
    if not text:
        return None

    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "")
            text = text.replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        text = text.replace(",", ".")

    try:
        amount = Decimal(text)
    except InvalidOperation:
        return None

    cents = (amount * Decimal("100")).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return int(cents)


def _canonical_side(value: object) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip().casefold()
    if not text:
        return None
    return SIDE_MAP.get(text, text.upper())


def _canonical_entry_type(value: object) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    return text.upper()


def _build_ref_data(df: pd.DataFrame, resolved: dict[str, str], present_ref_sources: list[str]) -> pd.Series:
    if not present_ref_sources:
        return pd.Series([""] * len(df), index=df.index, dtype="string")

    def join_row(row: pd.Series) -> str:
        parts: list[str] = []
        for source_name in present_ref_sources:
            src_col = resolved[source_name]
            value = row[src_col]
            if pd.isna(value):
                continue
            text = str(value).strip()
            if text:
                parts.append(text)
        return " | ".join(parts)

    return df.apply(join_row, axis=1)


def _prepare_source_df(file_path: Path, alias_index: dict[str, str]) -> tuple[pd.DataFrame, str]:
    df = pd.read_excel(file_path, skiprows=1)
    source_columns = [str(c) for c in df.columns]
    resolved = _resolve_columns(source_columns, alias_index)

    missing = [
        REQUIRED_SOURCE_FIELDS[key]
        for key in REQUIRED_SOURCE_FIELDS
        if REQUIRED_SOURCE_FIELDS[key] not in resolved
    ]
    if missing:
        raise SchemaError(
            code="CANONICAL_MISSING_COLUMN",
            message=f"Missing required columns: {', '.join(missing)}",
            hint="Run --stage schema and fix source headers first.",
            file_path=str(file_path),
        )

    rec_type = _detect_rec_type(df, resolved["Set ID"], file_path)
    present_ref_sources = [name for name in REFERENCE_SOURCE_FIELDS if name in resolved]

    prepared = pd.DataFrame(index=df.index)
    prepared["source_file"] = file_path.name
    prepared["tr_rec_name"] = rec_type
    prepared["Set ID"] = df[resolved["Set ID"]]
    prepared["Entry Type"] = df[resolved["Entry Type"]]
    prepared["Side"] = df[resolved["Side"]]
    prepared["Currency"] = df[resolved["Currency"]]
    prepared["Amount"] = df[resolved["Amount"]]
    prepared["Value Date"] = df[resolved["Value Date"]]
    prepared["item id"] = df[resolved["item id"]]
    prepared["tr_ref_data"] = _build_ref_data(df, resolved, present_ref_sources)

    for source_col in df.columns:
        if source_col not in prepared.columns:
            prepared[source_col] = df[source_col]

    return prepared, rec_type


def _clear_output_dir(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for child in output_dir.iterdir():
        if child.is_file() or child.is_symlink():
            child.unlink()
        elif child.is_dir():
            shutil.rmtree(child)


def run_canonical_stage(project_root: Path) -> int:
    stage_started = perf_counter()
    ingestion = discover_inputs(project_root)

    output_dir = project_root / "data" / "output"
    _clear_output_dir(output_dir)

    alias_index = _build_alias_index()

    prepared_frames: list[pd.DataFrame] = []
    roles: list[str] = []
    for rec_file in ingestion.rec_files:
        prepared, role = _prepare_source_df(rec_file, alias_index)
        prepared_frames.append(prepared)
        roles.append(role)

    roles = sorted(roles)
    if roles != ["GVA", "WSO"]:
        raise SchemaError(
            code="CANONICAL_ROLE_MISMATCH",
            message=f"Expected one GVA and one WSO file, got roles: {roles}",
            hint="Check Set ID values in source rec files.",
        )

    merged = pd.concat(prepared_frames, ignore_index=True)

    amounts = merged["Amount"].apply(_parse_amount_to_cents)
    amount_parse_errors = int((merged["Amount"].notna() & amounts.isna()).sum())
    if amount_parse_errors:
        raise SchemaError(
            code="CANONICAL_AMOUNT_PARSE_ERROR",
            message=f"Unparseable Amount values: {amount_parse_errors}",
            hint="Amount must be numeric (dot or comma decimal is accepted).",
        )

    parsed_dates = pd.to_datetime(merged["Value Date"], errors="coerce")
    date_parse_errors = int((merged["Value Date"].notna() & parsed_dates.isna()).sum())
    if date_parse_errors:
        raise SchemaError(
            code="CANONICAL_DATE_PARSE_ERROR",
            message=f"Unparseable Value Date values: {date_parse_errors}",
            hint="Use valid dates in Value Date column.",
        )

    sides = merged["Side"].apply(_canonical_side)
    side_unknown_count = int(sides.isna().sum())

    canonical = merged.copy()
    canonical["tr_found"] = False
    canonical["tr_fund_name"] = ""
    canonical["tr_issuer_name"] = ""
    canonical["tr_issuer_keys"] = ""
    canonical["tr_entry_type"] = merged["Entry Type"].apply(_canonical_entry_type)
    canonical["tr_side"] = sides
    canonical["tr_currency"] = merged["Currency"].astype("string").str.strip().str.upper()
    canonical["tr_amount_cents"] = amounts.astype("Int64")
    canonical["tr_value_date"] = parsed_dates.dt.strftime("%Y-%m-%d")
    canonical["tr_item_id"] = merged["item id"].astype("string").str.strip()

    output_columns = [col for col in TR_COLUMNS if col in canonical.columns] + [
        col for col in canonical.columns if col not in TR_COLUMNS
    ]
    canonical = canonical[output_columns]

    output_file_name = "gva_wso_matches_uniquenumber.xlsx"
    output_path = output_dir / output_file_name
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        canonical.to_excel(writer, sheet_name="canonical", index=False)

    metrics = CanonicalRunMetrics(
        rows_total=len(canonical),
        rows_gva=int((canonical["tr_rec_name"] == "GVA").sum()),
        rows_wso=int((canonical["tr_rec_name"] == "WSO").sum()),
        amount_parse_errors=amount_parse_errors,
        date_parse_errors=date_parse_errors,
        side_unknown_count=side_unknown_count,
        output_file_name=output_file_name,
        elapsed_ms=int((perf_counter() - stage_started) * 1000),
    )

    print("[canonical] status=ok")
    print(
        "[canonical] "
        f"rows_total={metrics.rows_total} rows_gva={metrics.rows_gva} rows_wso={metrics.rows_wso} "
        f"side_unknowns={metrics.side_unknown_count}"
    )
    print(f"[canonical] output_file={metrics.output_file_name}")
    print(f"[canonical] total_elapsed_ms={metrics.elapsed_ms}")
    return 0
