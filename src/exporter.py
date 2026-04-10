from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil

import pandas as pd

from config import OUTPUT_FILE_NAME, SHEET_FUND_NOT_FOUND, SHEET_GVA_WSO, SHEET_SUMMARY


@dataclass
class ExportResult:
    output_path: Path


def clear_output_dir(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for child in output_dir.iterdir():
        if child.is_file() or child.is_symlink():
            child.unlink()
        elif child.is_dir():
            shutil.rmtree(child)


def export_workbook(
    project_root: Path,
    dataset: pd.DataFrame,
    fund_not_found: pd.DataFrame,
    summary: pd.DataFrame,
) -> ExportResult:
    output_dir = project_root / "data" / "output"
    clear_output_dir(output_dir)

    output_path = output_dir / OUTPUT_FILE_NAME
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        dataset.to_excel(writer, sheet_name=SHEET_GVA_WSO, index=False)
        fund_not_found.to_excel(writer, sheet_name=SHEET_FUND_NOT_FOUND, index=False)
        summary.to_excel(writer, sheet_name=SHEET_SUMMARY, index=False)

    return ExportResult(output_path=output_path)
