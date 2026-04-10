from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

import pandas as pd

from config import (
    KEY_VALUES_ISSUER_COL_CANDIDATES,
    KEY_VALUES_KEY_COL_CANDIDATES,
    MAPPING_FUND_COL_CANDIDATES,
    MAPPING_SET_ID_COL_CANDIDATES,
)
from schema_errors import SchemaError


@dataclass
class InputFiles:
    rec_files: list[Path]
    mapping_file: Path
    key_values_file: Path
    elapsed_ms: int


@dataclass
class LoadedInputs:
    rec_frames: list[pd.DataFrame]
    rec_file_names: list[str]
    mapping_df: pd.DataFrame
    key_values_df: pd.DataFrame
    mapping_set_id_col: str
    mapping_fund_col: str
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


def discover_files(project_root: Path) -> InputFiles:
    started = perf_counter()
    input_dir = project_root / "data" / "input"
    static_dir = project_root / "data" / "static"

    if not input_dir.exists():
        raise SchemaError(
            code="LOADER_INPUT_DIR_MISSING",
            message=f"Input directory does not exist: {input_dir}",
            hint="Create data/input and place rec xlsx files there.",
        )
    if not static_dir.exists():
        raise SchemaError(
            code="LOADER_STATIC_DIR_MISSING",
            message=f"Static directory does not exist: {static_dir}",
            hint="Create data/static and place mapping/key files there.",
        )

    rec_files = _xlsx_files(input_dir)
    if len(rec_files) != 2:
        raise SchemaError(
            code="LOADER_REC_FILE_COUNT",
            message=f"Expected exactly 2 rec files in data/input, found {len(rec_files)}",
            hint="Keep only two rec xlsx files in data/input.",
        )

    static_files = _xlsx_files(static_dir)
    if not static_files:
        raise SchemaError(
            code="LOADER_NO_STATIC_FILES",
            message="No .xlsx files found in data/static",
            hint="Add mapping and key-values files.",
        )

    mapping_file = _find_single_by_tokens(static_files, ("mapping",), "LOADER_MAPPING_NOT_FOUND", "mapping")
    key_values_file = _find_single_by_tokens(
        static_files,
        ("key", "keys", "key_values", "key-values"),
        "LOADER_KEYS_NOT_FOUND",
        "key-values",
    )

    used = {mapping_file, key_values_file}
    unexpected = [p.name for p in static_files if p not in used]
    if unexpected:
        raise SchemaError(
            code="LOADER_UNEXPECTED_STATIC_FILES",
            message=f"Unexpected static files: {', '.join(unexpected)}",
            hint="Keep only mapping and key-values xlsx files in data/static.",
        )

    return InputFiles(
        rec_files=rec_files,
        mapping_file=mapping_file,
        key_values_file=key_values_file,
        elapsed_ms=int((perf_counter() - started) * 1000),
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


def load_datasets(files: InputFiles) -> LoadedInputs:
    rec_frames = [pd.read_excel(path, skiprows=1) for path in files.rec_files]
    mapping_df = pd.read_excel(files.mapping_file)
    key_values_df = pd.read_excel(files.key_values_file)

    mapping_set_id_col = _pick_column(
        [str(c) for c in mapping_df.columns],
        MAPPING_SET_ID_COL_CANDIDATES,
        "LOADER_MAPPING_SET_ID_COL_MISSING",
        "mapping set-id",
    )
    mapping_fund_col = _pick_column(
        [str(c) for c in mapping_df.columns],
        MAPPING_FUND_COL_CANDIDATES,
        "LOADER_MAPPING_FUND_COL_MISSING",
        "mapping fund-name",
    )
    key_issuer_col = _pick_column(
        [str(c) for c in key_values_df.columns],
        KEY_VALUES_ISSUER_COL_CANDIDATES,
        "LOADER_KEY_ISSUER_COL_MISSING",
        "key-values issuer",
    )
    key_value_col = _pick_column(
        [str(c) for c in key_values_df.columns],
        KEY_VALUES_KEY_COL_CANDIDATES,
        "LOADER_KEY_VALUE_COL_MISSING",
        "key-values key",
    )

    return LoadedInputs(
        rec_frames=rec_frames,
        rec_file_names=[p.name for p in files.rec_files],
        mapping_df=mapping_df,
        key_values_df=key_values_df,
        mapping_set_id_col=mapping_set_id_col,
        mapping_fund_col=mapping_fund_col,
        key_issuer_col=key_issuer_col,
        key_value_col=key_value_col,
    )
