from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass
class TiersResult:
    dataset: pd.DataFrame
    matches: pd.DataFrame
    tier_a_count: int
    tier_b_count: int


def _build_exact_key(df: pd.DataFrame, include_item_id: bool) -> pd.Series:
    cols = ["tr_fund_name", "tr_side", "tr_currency", "tr_amount_cents", "tr_value_date"]
    if include_item_id:
        cols.append("tr_item_id")
    parts = []
    for col in cols:
        parts.append(df[col].astype("string").fillna(""))
    key = parts[0]
    for part in parts[1:]:
        key = key + "|" + part
    return key


def _pair_rows(left_idx: list[int], right_idx: list[int], tier_label: str) -> tuple[list[dict[str, object]], list[int], list[int]]:
    pair_count = min(len(left_idx), len(right_idx))
    match_rows: list[dict[str, object]] = []
    for i in range(pair_count):
        match_rows.append(
            {
                "tier": tier_label,
                "gva_index": left_idx[i],
                "wso_index": right_idx[i],
            }
        )
    return match_rows, left_idx[pair_count:], right_idx[pair_count:]


def _match_exact(dataset: pd.DataFrame, include_item_id: bool, tier_label: str, active_mask: pd.Series) -> tuple[pd.DataFrame, list[dict[str, object]]]:
    df = dataset.copy()
    active = df[active_mask].copy()
    active["_exact_key"] = _build_exact_key(active, include_item_id)

    grouped_gva = active[active["tr_rec_name"] == "GVA"].groupby("_exact_key", sort=True)
    grouped_wso = active[active["tr_rec_name"] == "WSO"].groupby("_exact_key", sort=True)

    matches: list[dict[str, object]] = []
    for key in sorted(set(grouped_gva.groups.keys()) & set(grouped_wso.groups.keys())):
        gva_idx = sorted(grouped_gva.groups[key].tolist())
        wso_idx = sorted(grouped_wso.groups[key].tolist())
        matched, _, _ = _pair_rows(gva_idx, wso_idx, tier_label)
        matches.extend(matched)

    if matches:
        all_matched = [m["gva_index"] for m in matches] + [m["wso_index"] for m in matches]
        dataset.loc[all_matched, "tr_found"] = True

    return dataset, matches


def run_tiers_pipeline(dataset: pd.DataFrame) -> TiersResult:
    result = dataset.copy()
    result["tr_found"] = result["tr_found"].fillna(False).astype(bool)

    tier_a_mask = (~result["tr_found"]) & result["tr_item_id"].astype("string").str.len().gt(0)
    result, tier_a_matches = _match_exact(result, include_item_id=True, tier_label="A", active_mask=tier_a_mask)

    tier_b_mask = (~result["tr_found"])
    result, tier_b_matches = _match_exact(result, include_item_id=False, tier_label="B", active_mask=tier_b_mask)

    all_matches = pd.DataFrame(tier_a_matches + tier_b_matches)
    if all_matches.empty:
        all_matches = pd.DataFrame(columns=["tier", "gva_index", "wso_index"])

    return TiersResult(
        dataset=result,
        matches=all_matches,
        tier_a_count=len(tier_a_matches),
        tier_b_count=len(tier_b_matches),
    )
