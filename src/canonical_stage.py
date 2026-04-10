from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

import pandas as pd

from exporter import export_workbook
from fund_finder import apply_fund_mapping
from issuer_assigner import assign_issuers
from loader import discover_files, load_datasets
from normalize import normalize_inputs
from tiers_pipeline import run_tiers_pipeline


@dataclass
class PipelineMetrics:
    rows_total: int
    rows_gva: int
    rows_wso: int
    fund_found: int
    fund_not_found: int
    issuer_assigned: int
    tier_a_matches: int
    tier_b_matches: int
    elapsed_ms: int


def _build_matches_sheet(dataset: pd.DataFrame, matches: pd.DataFrame) -> pd.DataFrame:
    if matches.empty:
        return pd.DataFrame(
            columns=[
                "tier",
                "gva_set_id",
                "wso_set_id",
                "gva_item_id",
                "wso_item_id",
                "amount_cents",
                "value_date",
                "fund_name",
            ]
        )

    rows: list[dict[str, object]] = []
    for _, pair in matches.iterrows():
        gva_row = dataset.loc[int(pair["gva_index"])]
        wso_row = dataset.loc[int(pair["wso_index"])]
        rows.append(
            {
                "tier": pair["tier"],
                "gva_set_id": gva_row.get("Set ID", ""),
                "wso_set_id": wso_row.get("Set ID", ""),
                "gva_item_id": gva_row.get("tr_item_id", ""),
                "wso_item_id": wso_row.get("tr_item_id", ""),
                "amount_cents": gva_row.get("tr_amount_cents", ""),
                "value_date": gva_row.get("tr_value_date", ""),
                "fund_name": gva_row.get("tr_fund_name", ""),
            }
        )
    return pd.DataFrame(rows)


def _build_summary(metrics: PipelineMetrics) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"metric": "rows_total", "value": metrics.rows_total},
            {"metric": "rows_gva", "value": metrics.rows_gva},
            {"metric": "rows_wso", "value": metrics.rows_wso},
            {"metric": "fund_found", "value": metrics.fund_found},
            {"metric": "fund_not_found", "value": metrics.fund_not_found},
            {"metric": "issuer_assigned", "value": metrics.issuer_assigned},
            {"metric": "tier_a_matches", "value": metrics.tier_a_matches},
            {"metric": "tier_b_matches", "value": metrics.tier_b_matches},
            {"metric": "elapsed_ms", "value": metrics.elapsed_ms},
        ]
    )


def run_canonical_stage(project_root: Path) -> int:
    started = perf_counter()

    files = discover_files(project_root)
    loaded = load_datasets(files)

    normalized = normalize_inputs(loaded, files.rec_files)
    funded = apply_fund_mapping(normalized, loaded)
    issuered = assign_issuers(funded.dataset, normalized.key_values_df, loaded)
    tiers = run_tiers_pipeline(issuered.dataset)

    metrics = PipelineMetrics(
        rows_total=len(tiers.dataset),
        rows_gva=normalized.rows_gva,
        rows_wso=normalized.rows_wso,
        fund_found=funded.coverage_found,
        fund_not_found=funded.coverage_not_found,
        issuer_assigned=issuered.issuer_assigned_count,
        tier_a_matches=tiers.tier_a_count,
        tier_b_matches=tiers.tier_b_count,
        elapsed_ms=int((perf_counter() - started) * 1000),
    )

    matches_sheet = _build_matches_sheet(tiers.dataset, tiers.matches)
    summary_sheet = _build_summary(metrics)

    exported = export_workbook(
        project_root=project_root,
        dataset=tiers.dataset,
        fund_not_found=funded.fund_not_found,
        matches=matches_sheet,
        summary=summary_sheet,
    )

    print("[pipeline] status=ok")
    print(
        "[pipeline] "
        f"rows_total={metrics.rows_total} rows_gva={metrics.rows_gva} rows_wso={metrics.rows_wso} "
        f"fund_found={metrics.fund_found} fund_not_found={metrics.fund_not_found} "
        f"issuer_assigned={metrics.issuer_assigned}"
    )
    print(
        "[pipeline] "
        f"tier_a_matches={metrics.tier_a_matches} tier_b_matches={metrics.tier_b_matches} "
        f"elapsed_ms={metrics.elapsed_ms}"
    )
    print(f"[pipeline] output_file={exported.output_path.name}")
    return 0
