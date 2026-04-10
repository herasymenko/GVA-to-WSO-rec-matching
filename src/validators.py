from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
import re
import unicodedata

import pandas as pd

from config import REQUIRED_SOURCE_FIELDS, SIDE_MAP, SOURCE_ALIASES
from schema_errors import SchemaError


def normalize_header(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = " ".join(text.strip().split())
    return text.casefold()


def build_alias_index() -> dict[str, str]:
    index: dict[str, str] = {}
    for canonical, aliases in SOURCE_ALIASES.items():
        for alias in aliases:
            index[normalize_header(alias)] = canonical
    return index


def resolve_columns(columns: list[str], alias_index: dict[str, str]) -> dict[str, str]:
    resolved: dict[str, str] = {}
    for source_col in columns:
        canonical = alias_index.get(normalize_header(source_col))
        if canonical and canonical not in resolved:
            resolved[canonical] = source_col
    return resolved


def ensure_required_columns(resolved: dict[str, str], file_path: Path, code_prefix: str) -> None:
    missing = [
        REQUIRED_SOURCE_FIELDS[key]
        for key in REQUIRED_SOURCE_FIELDS
        if REQUIRED_SOURCE_FIELDS[key] not in resolved
    ]
    if missing:
        raise SchemaError(
            code=f"{code_prefix}_MISSING_COLUMN",
            message=f"Missing required columns: {', '.join(missing)}",
            hint="Check source header names and aliases.",
            file_path=str(file_path),
        )


def detect_rec_type(df: pd.DataFrame, set_id_col: str, file_path: Path, code_prefix: str) -> str:
    series = df[set_id_col].dropna().astype(str).str.strip().str.upper()
    sample = series.head(250)
    if sample.empty:
        raise SchemaError(
            code=f"{code_prefix}_EMPTY_SET_ID",
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
        code=f"{code_prefix}_REC_TYPE_AMBIGUOUS",
        message="Could not detect whether rec file is WSO or GVA",
        hint="Expected dominant Set ID prefixes AIS... or GVA...",
        file_path=str(file_path),
    )


def parse_amount_to_cents(value: object) -> int | None:
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


def canonical_side(value: object) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip().casefold()
    if not text:
        return None
    return SIDE_MAP.get(text, text.upper())


def canonical_entry_type(value: object) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    return text[:3].upper()


def canonical_item_id(value: object) -> int | str | None:
    if pd.isna(value):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith(".0"):
        integer_part = text[:-2]
        if integer_part.isdigit() or (integer_part.startswith("-") and integer_part[1:].isdigit()):
            return int(integer_part)
    if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
        return int(text)
    return text


def clean_ref_text(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip().casefold()
    text = re.sub(r"[^a-z0-9 ]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text
