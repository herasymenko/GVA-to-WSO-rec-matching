from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

import pandas as pd

from exporter import export_workbook
from loader import discover_rec_files, load_rec_datasets
from normalize import normalize_inputs


@dataclass
class PipelineMetrics:
    rows_total: int
    rows_gva: int
    rows_wso: int
    elapsed_ms: int


def _build_summary(metrics: PipelineMetrics) -> pd.DataFrame:
    return pd.DataFrame(columns=["metric", "value"])


def run_canonical_stage(project_root: Path) -> int:
    started = perf_counter()

    rec_files = discover_rec_files(project_root)
    loaded = load_rec_datasets(rec_files)

    normalized = normalize_inputs(loaded, rec_files)

    metrics = PipelineMetrics(
        rows_total=len(normalized.normalized_df),
        rows_gva=normalized.rows_gva,
        rows_wso=normalized.rows_wso,
        elapsed_ms=int((perf_counter() - started) * 1000),
    )

    summary_sheet = _build_summary(metrics)
    fund_not_found_sheet = pd.DataFrame()

    exported = export_workbook(
        project_root=project_root,
        dataset=normalized.normalized_df,
        fund_not_found=fund_not_found_sheet,
        summary=summary_sheet,
    )

    print("[pipeline] status=ok")
    print(
        "[pipeline] "
        f"rows_total={metrics.rows_total} rows_gva={metrics.rows_gva} rows_wso={metrics.rows_wso} elapsed_ms={metrics.elapsed_ms}"
    )
    print(f"[pipeline] output_file={exported.output_path.name}")
    return 0
