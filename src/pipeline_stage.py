from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

import pandas as pd

from exporter import export_workbook
from fund_finder import apply_fund_mapping
from issuer_assigner import assign_issuers
from loader import (
    discover_key_values_file,
    discover_mapping_file,
    discover_rec_files,
    load_key_values_dataset,
    load_mapping_dataset,
    load_rec_datasets,
)
from normalize import normalize_inputs
from tiers_pipeline import run_tiers_pipeline


@dataclass
class PipelineMetrics:
    rows_total: int
    rows_gva: int
    rows_wso: int
    fund_not_found_rows: int
    issuer_assigned_rows: int
    summary_rows: int
    tier_a_matches: int
    tier_b_matches: int
    load_ms: int
    normalize_ms: int
    mapping_ms: int
    issuer_ms: int
    tiers_ms: int
    export_ms: int
    elapsed_ms: int


def run_pipeline_stage(project_root: Path) -> int:
    started = perf_counter()

    t0 = perf_counter()
    rec_files = discover_rec_files(project_root)
    mapping_file = discover_mapping_file(project_root)
    key_values_file = discover_key_values_file(project_root)
    loaded = load_rec_datasets(rec_files)
    mapping_loaded = load_mapping_dataset(mapping_file)
    key_values_loaded = load_key_values_dataset(key_values_file)
    load_ms = int((perf_counter() - t0) * 1000)

    t1 = perf_counter()
    normalized = normalize_inputs(loaded, rec_files)
    normalize_ms = int((perf_counter() - t1) * 1000)

    t2 = perf_counter()
    funded = apply_fund_mapping(normalized, mapping_loaded)
    mapping_ms = int((perf_counter() - t2) * 1000)

    t3 = perf_counter()
    issuered_main = assign_issuers(funded.dataset, key_values_loaded.key_values_df, key_values_loaded)
    issuered_missing = assign_issuers(funded.fund_not_found, key_values_loaded.key_values_df, key_values_loaded)
    issuer_ms = int((perf_counter() - t3) * 1000)

    t4 = perf_counter()
    tiers_result = run_tiers_pipeline(issuered_main.dataset)
    tiers_ms = int((perf_counter() - t4) * 1000)
    summary_sheet = tiers_result.summary

    t5 = perf_counter()
    exported = export_workbook(
        project_root=project_root,
        dataset=tiers_result.dataset,
        fund_not_found=issuered_missing.dataset,
        summary=summary_sheet,
    )
    export_ms = int((perf_counter() - t5) * 1000)

    metrics = PipelineMetrics(
        rows_total=len(tiers_result.dataset),
        rows_gva=int((tiers_result.dataset["tr_rec_name"] == "GVA").sum()),
        rows_wso=int((tiers_result.dataset["tr_rec_name"] == "WSO").sum()),
        fund_not_found_rows=len(issuered_missing.dataset),
        issuer_assigned_rows=issuered_main.issuer_assigned_count + issuered_missing.issuer_assigned_count,
        summary_rows=len(summary_sheet),
        tier_a_matches=int(tiers_result.tier_counts.get("A", 0)),
        tier_b_matches=int(tiers_result.tier_counts.get("B", 0)),
        load_ms=load_ms,
        normalize_ms=normalize_ms,
        mapping_ms=mapping_ms,
        issuer_ms=issuer_ms,
        tiers_ms=tiers_ms,
        export_ms=export_ms,
        elapsed_ms=int((perf_counter() - started) * 1000),
    )

    print("[pipeline] status=ok")
    print(
        "[pipeline] "
        f"rows_total={metrics.rows_total} rows_gva={metrics.rows_gva} rows_wso={metrics.rows_wso} "
        f"fund_not_found_rows={metrics.fund_not_found_rows} issuer_assigned_rows={metrics.issuer_assigned_rows} "
        f"summary_rows={metrics.summary_rows} tier_a_matches={metrics.tier_a_matches} "
        f"tier_b_matches={metrics.tier_b_matches} "
        f"elapsed_ms={metrics.elapsed_ms}"
    )
    print(
        "[pipeline] "
        f"load_ms={metrics.load_ms} normalize_ms={metrics.normalize_ms} "
        f"mapping_ms={metrics.mapping_ms} issuer_ms={metrics.issuer_ms} tiers_ms={metrics.tiers_ms} "
        f"export_ms={metrics.export_ms}"
    )
    print(f"[pipeline] output_file={exported.output_path.name}")
    return 0
