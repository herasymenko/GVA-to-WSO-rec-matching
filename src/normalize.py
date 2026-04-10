from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from config import REFERENCE_SOURCE_FIELDS, REQUIRED_SOURCE_FIELDS, TR_COLUMNS
from loader import LoadedRecInputs
from schema_errors import SchemaError
from validators import (
    build_alias_index,
    canonical_entry_type,
    canonical_item_id,
    canonical_side,
    clean_ref_text,
    detect_rec_type,
    ensure_required_columns,
    parse_amount_to_cents,
    resolve_columns,
)


@dataclass
class NormalizeResult:
    normalized_df: pd.DataFrame
    rows_gva: int
    rows_wso: int


def _build_ref_data(df: pd.DataFrame, resolved: dict[str, str], present_ref_sources: list[str]) -> pd.Series:
    if not present_ref_sources:
        return pd.Series([""] * len(df), index=df.index, dtype="string")

    def join_row(row: pd.Series) -> str:
        parts: list[str] = []
        for source_name in present_ref_sources:
            value = row[resolved[source_name]]
            cleaned = clean_ref_text(value)
            if cleaned:
                parts.append(cleaned)
        return " | ".join(parts)

    return df.apply(join_row, axis=1)


def normalize_inputs(loaded: LoadedRecInputs, rec_paths: list[Path]) -> NormalizeResult:
    alias_index = build_alias_index()

    prepared_frames: list[pd.DataFrame] = []
    roles: list[str] = []

    for i, source_df in enumerate(loaded.rec_frames):
        file_path = rec_paths[i]
        df = source_df.copy()
        source_columns = [str(c) for c in df.columns]
        resolved = resolve_columns(source_columns, alias_index)
        ensure_required_columns(resolved, file_path, "NORMALIZE")

        rename_map = {
            source_name: canonical_name
            for canonical_name, source_name in resolved.items()
            if source_name != canonical_name and canonical_name not in df.columns
        }
        if rename_map:
            df = df.rename(columns=rename_map)
            resolved = {
                canonical_name: rename_map.get(source_name, source_name)
                for canonical_name, source_name in resolved.items()
            }

        rec_type = detect_rec_type(df, resolved["Set ID"], file_path, "NORMALIZE")
        roles.append(rec_type)

        present_ref_sources = [name for name in REFERENCE_SOURCE_FIELDS if name in resolved]

        prepared = df.copy()
        prepared["tr_rec_name"] = rec_type
        prepared["tr_ref_data"] = _build_ref_data(df, resolved, present_ref_sources)
        prepared["tr_found"] = False
        prepared["tr_fund_name"] = ""
        prepared["tr_issuer_name"] = ""
        prepared["tr_issuer_keys"] = ""
        prepared["tr_entry_type"] = df[resolved["Entry Type"]].apply(canonical_entry_type)
        prepared["tr_side"] = df[resolved["Side"]].apply(canonical_side)
        prepared["tr_currency"] = df[resolved["Currency"]].astype("string").str.strip().str.upper()

        amounts = df[resolved["Amount"]].apply(parse_amount_to_cents)
        amount_parse_errors = int((df[resolved["Amount"]].notna() & amounts.isna()).sum())
        if amount_parse_errors:
            raise SchemaError(
                code="NORMALIZE_AMOUNT_PARSE_ERROR",
                message=f"Unparseable Amount values: {amount_parse_errors}",
                hint="Amount must be numeric (dot or comma decimal is accepted).",
                file_path=str(file_path),
            )
        prepared["tr_amount_cents"] = amounts.astype("Int64")

        parsed_dates = pd.to_datetime(df[resolved["Value Date"]], errors="coerce")
        date_parse_errors = int((df[resolved["Value Date"]].notna() & parsed_dates.isna()).sum())
        if date_parse_errors:
            raise SchemaError(
                code="NORMALIZE_DATE_PARSE_ERROR",
                message=f"Unparseable Value Date values: {date_parse_errors}",
                hint="Use valid dates in Value Date column.",
                file_path=str(file_path),
            )
        prepared["tr_value_date"] = parsed_dates.dt.strftime("%Y-%m-%d")

        prepared["tr_item_id"] = df[resolved["item id"]].apply(canonical_item_id)
        prepared_frames.append(prepared)

    if sorted(roles) != ["GVA", "WSO"]:
        raise SchemaError(
            code="NORMALIZE_ROLE_MISMATCH",
            message=f"Expected one GVA and one WSO source file, got roles: {sorted(roles)}",
            hint="Verify Set ID prefixes in source rec files.",
        )

    merged = pd.concat(prepared_frames, ignore_index=True)
    output_columns = [col for col in TR_COLUMNS if col in merged.columns] + [
        col for col in merged.columns if col not in TR_COLUMNS
    ]
    merged = merged[output_columns]

    rows_gva = int((merged["tr_rec_name"] == "GVA").sum())
    rows_wso = int((merged["tr_rec_name"] == "WSO").sum())

    return NormalizeResult(
        normalized_df=merged,
        rows_gva=rows_gva,
        rows_wso=rows_wso,
    )
