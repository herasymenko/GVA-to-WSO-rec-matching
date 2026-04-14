from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from config import (
    KEY_VALUES_ISSUER_COL_CANDIDATES,
    KEY_VALUES_KEY_COL_CANDIDATES,
    MAPPING_FUND_COL_CANDIDATES,
    MAPPING_GVA_SET_ID_COL_CANDIDATES,
    MAPPING_GVA_SHEET_NAME,
    MAPPING_WSO_SET_ID_COL_CANDIDATES,
    MAPPING_WSO_SHEET_NAME,
)
from schema_errors import SchemaError


@dataclass
class LoadedRecInputs:
    rec_frames: list[pd.DataFrame]
    rec_file_names: list[str]


@dataclass
class LoadedMappingInputs:
    wso_mapping_df: pd.DataFrame
    wso_mapping_set_id_col: str
    gva_mapping_df: pd.DataFrame
    gva_mapping_set_id_col: str
    mapping_fund_col: str


@dataclass
class LoadedKeyValuesInputs:
    key_values_df: pd.DataFrame
    key_issuer_col: str
    key_value_col: str


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


def discover_rec_files(project_root: Path) -> list[Path]:
    input_dir = project_root / "data" / "input"
    if not input_dir.exists():
        raise SchemaError(
            code="LOADER_INPUT_DIR_MISSING",
            message=f"Input directory does not exist: {input_dir}",
            hint="Create data/input and place rec xlsx files there.",
        )

    rec_files = _xlsx_files(input_dir)
    if len(rec_files) != 2:
        raise SchemaError(
            code="LOADER_REC_FILE_COUNT",
            message=f"Expected exactly 2 rec files in data/input, found {len(rec_files)}",
            hint="Keep only two rec xlsx files in data/input.",
        )
    return rec_files


def discover_mapping_file(project_root: Path) -> Path:
    static_dir = project_root / "data" / "static"
    if not static_dir.exists():
        raise SchemaError(
            code="LOADER_STATIC_DIR_MISSING",
            message=f"Static directory does not exist: {static_dir}",
            hint="Create data/static and place mapping xlsx file there.",
        )

    static_files = _xlsx_files(static_dir)
    if not static_files:
        raise SchemaError(
            code="LOADER_NO_STATIC_FILES",
            message="No .xlsx files found in data/static",
            hint="Add mapping xlsx file.",
        )

    return _find_single_by_tokens(static_files, ("mapping",), "LOADER_MAPPING_NOT_FOUND", "mapping")


def discover_key_values_file(project_root: Path) -> Path:
    static_dir = project_root / "data" / "static"
    if not static_dir.exists():
        raise SchemaError(
            code="LOADER_STATIC_DIR_MISSING",
            message=f"Static directory does not exist: {static_dir}",
            hint="Create data/static and place key-values xlsx file there.",
        )

    static_files = _xlsx_files(static_dir)
    if not static_files:
        raise SchemaError(
            code="LOADER_NO_STATIC_FILES",
            message="No .xlsx files found in data/static",
            hint="Add key-values xlsx file.",
        )

    return _find_single_by_tokens(
        static_files,
        ("key_values", "key-values", "keys", "key"),
        "LOADER_KEY_VALUES_NOT_FOUND",
        "key-values",
    )


def _pick_column(columns: list[str], candidates: list[str], code: str, kind: str) -> str:
    lower_map = {c.casefold(): c for c in columns}
    for candidate in candidates:
        found = lower_map.get(candidate.casefold())
        if found:
            return found
    raise SchemaError(
        code=code,
        message=f"Could not find {kind} column. Found columns: {', '.join(columns)}",
        hint=f"Expected one of: {', '.join(candidates)}",
    )


def load_rec_datasets(rec_files: list[Path]) -> LoadedRecInputs:
    return LoadedRecInputs(
        rec_frames=[pd.read_excel(path, skiprows=1) for path in rec_files],
        rec_file_names=[path.name for path in rec_files],
    )


def load_mapping_dataset(mapping_file: Path) -> LoadedMappingInputs:
    workbook = pd.ExcelFile(mapping_file)
    if MAPPING_WSO_SHEET_NAME not in workbook.sheet_names:
        raise SchemaError(
            code="LOADER_MAPPING_WSO_SHEET_MISSING",
            message=f"Mapping workbook does not contain sheet: {MAPPING_WSO_SHEET_NAME}",
            hint="Add sheet named WSO to mapping workbook.",
        )
    if MAPPING_GVA_SHEET_NAME not in workbook.sheet_names:
        raise SchemaError(
            code="LOADER_MAPPING_GVA_SHEET_MISSING",
            message=f"Mapping workbook does not contain sheet: {MAPPING_GVA_SHEET_NAME}",
            hint="Add sheet named GVA to mapping workbook.",
        )

    wso_mapping_df = pd.read_excel(mapping_file, sheet_name=MAPPING_WSO_SHEET_NAME)
    gva_mapping_df = pd.read_excel(mapping_file, sheet_name=MAPPING_GVA_SHEET_NAME)

    wso_columns = [str(c) for c in wso_mapping_df.columns]
    gva_columns = [str(c) for c in gva_mapping_df.columns]

    wso_mapping_set_id_col = _pick_column(
        wso_columns,
        MAPPING_WSO_SET_ID_COL_CANDIDATES,
        "LOADER_MAPPING_WSO_SET_ID_COL_MISSING",
        "WSO mapping set-id",
    )
    gva_mapping_set_id_col = _pick_column(
        gva_columns,
        MAPPING_GVA_SET_ID_COL_CANDIDATES,
        "LOADER_MAPPING_GVA_SET_ID_COL_MISSING",
        "GVA mapping set-id",
    )
    mapping_fund_col = _pick_column(
        wso_columns,
        MAPPING_FUND_COL_CANDIDATES,
        "LOADER_MAPPING_FUND_COL_MISSING",
        "mapping fund-name",
    )
    _pick_column(
        gva_columns,
        [mapping_fund_col] + MAPPING_FUND_COL_CANDIDATES,
        "LOADER_MAPPING_GVA_FUND_COL_MISSING",
        "GVA mapping fund-name",
    )

    return LoadedMappingInputs(
        wso_mapping_df=wso_mapping_df,
        wso_mapping_set_id_col=wso_mapping_set_id_col,
        gva_mapping_df=gva_mapping_df,
        gva_mapping_set_id_col=gva_mapping_set_id_col,
        mapping_fund_col=mapping_fund_col,
    )


def load_key_values_dataset(key_values_file: Path) -> LoadedKeyValuesInputs:
    key_values_df = pd.read_excel(key_values_file)
    columns = [str(c) for c in key_values_df.columns]

    key_issuer_col = _pick_column(
        columns,
        KEY_VALUES_ISSUER_COL_CANDIDATES,
        "LOADER_KEY_VALUES_ISSUER_COL_MISSING",
        "key-values issuer",
    )
    key_value_col = _pick_column(
        columns,
        KEY_VALUES_KEY_COL_CANDIDATES,
        "LOADER_KEY_VALUES_KEY_COL_MISSING",
        "key-values key",
    )

    return LoadedKeyValuesInputs(
        key_values_df=key_values_df,
        key_issuer_col=key_issuer_col,
        key_value_col=key_value_col,
    )
