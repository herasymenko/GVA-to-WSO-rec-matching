from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

import pandas as pd

from exporter import export_workbook
from fund_finder import apply_fund_mapping
from loader import discover_mapping_file, discover_rec_files, load_mapping_dataset, load_rec_datasets
from normalize import normalize_inputs


@dataclass
class PipelineMetrics:
    rows_total: int
    rows_gva: int
    rows_wso: int
    fund_not_found_rows: int
    elapsed_ms: int


def _build_summary(metrics: PipelineMetrics) -> pd.DataFrame:
    return pd.DataFrame()


def run_pipeline_stage(project_root: Path) -> int:
    started = perf_counter()

    rec_files = discover_rec_files(project_root)
    mapping_file = discover_mapping_file(project_root)
    loaded = load_rec_datasets(rec_files)
    mapping_loaded = load_mapping_dataset(mapping_file)

    normalized = normalize_inputs(loaded, rec_files)
    funded = apply_fund_mapping(normalized, mapping_loaded)

    metrics = PipelineMetrics(
        rows_total=len(funded.dataset),
        rows_gva=int((funded.dataset["tr_rec_name"] == "GVA").sum()),
        rows_wso=int((funded.dataset["tr_rec_name"] == "WSO").sum()),
        fund_not_found_rows=len(funded.fund_not_found),
        elapsed_ms=int((perf_counter() - started) * 1000),
    )

    summary_sheet = _build_summary(metrics)

    exported = export_workbook(
        project_root=project_root,
        dataset=funded.dataset,
        fund_not_found=funded.fund_not_found,
        summary=summary_sheet,
    )

    print("[pipeline] status=ok")
    print(
        "[pipeline] "
        f"rows_total={metrics.rows_total} rows_gva={metrics.rows_gva} rows_wso={metrics.rows_wso} "
        f"fund_not_found_rows={metrics.fund_not_found_rows} elapsed_ms={metrics.elapsed_ms}"
    )
    print(f"[pipeline] output_file={exported.output_path.name}")
    return 0
