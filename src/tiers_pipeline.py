from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import pandas as pd

from config import SUMMARY_COLUMNS


@dataclass
class TiersResult:
    dataset: pd.DataFrame
    summary: pd.DataFrame
    tier_counts: dict[str, int]
    total_matches: int


@dataclass(frozen=True)
class TierDefinition:
    label: str
    explanation: str
    enabled: bool
    runner: Callable[[pd.DataFrame, "TierDefinition"], tuple[pd.DataFrame, pd.DataFrame, int]]


def _series_or_empty(df: pd.DataFrame, col: str) -> pd.Series:
    if col in df.columns:
        return df[col]
    return pd.Series("", index=df.index, dtype="string")


def _empty_summary() -> pd.DataFrame:
    return pd.DataFrame(columns=SUMMARY_COLUMNS)


def _build_exact_key(df: pd.DataFrame, include_issuer: bool) -> pd.Series:
    key_cols = ["tr_fund_name", "tr_side", "tr_currency", "tr_amount_cents", "tr_value_date"]
    if include_issuer:
        key_cols.insert(1, "tr_issuer_name")

    parts = [df[col].astype("string").fillna("") for col in key_cols]
    key = parts[0]
    for part in parts[1:]:
        key = key + "|" + part
    return key


def _pair_by_key(gva: pd.DataFrame, wso: pd.DataFrame, key_col: str) -> pd.DataFrame:
    if gva.empty or wso.empty:
        return pd.DataFrame()

    left = gva.sort_index().copy()
    right = wso.sort_index().copy()

    left["_pair_seq"] = left.groupby(key_col, sort=False).cumcount()
    right["_pair_seq"] = right.groupby(key_col, sort=False).cumcount()
    left["_row_index_gva"] = left.index
    right["_row_index_wso"] = right.index

    pairs = left.merge(
        right,
        how="inner",
        on=[key_col, "_pair_seq"],
        suffixes=("_gva", "_wso"),
        sort=False,
    )
    if pairs.empty:
        return pairs

    return pairs.sort_values(by=[key_col, "_pair_seq", "_row_index_gva", "_row_index_wso"], kind="stable")


def _drop_matched(df: pd.DataFrame, pairs: pd.DataFrame, matched_index_col: str) -> pd.DataFrame:
    if df.empty or pairs.empty:
        return df
    return df.loc[~df.index.isin(pairs[matched_index_col].to_numpy())]


def _build_tier_a_summary(pairs: pd.DataFrame, tier: TierDefinition) -> pd.DataFrame:
    if pairs.empty:
        return _empty_summary()

    summary = pd.DataFrame(index=pairs.index)
    summary["tr_tier"] = tier.label
    summary["tr_tier_explanation"] = tier.explanation
    summary["tr_gva_item_id"] = _series_or_empty(pairs, "tr_item_id_gva")
    summary["tr_wso_item_id"] = _series_or_empty(pairs, "tr_item_id_wso")
    summary["tr_fund_name"] = _series_or_empty(pairs, "tr_fund_name_gva")
    if tier.label == "B":
        gva_issuer = _series_or_empty(pairs, "tr_issuer_name_gva").astype("string").str.strip()
        wso_issuer = _series_or_empty(pairs, "tr_issuer_name_wso").astype("string").str.strip()
        summary["tr_issuer_name"] = gva_issuer.where(gva_issuer.ne(""), wso_issuer)
    else:
        summary["tr_issuer_name"] = _series_or_empty(pairs, "tr_issuer_name_gva")
    summary["tr_entry_type"] = _series_or_empty(pairs, "tr_entry_type_gva")
    summary["tr_currency"] = _series_or_empty(pairs, "tr_currency_gva")
    summary["tr_gva_amount"] = _series_or_empty(pairs, "Amount_gva")
    summary["tr_wso_amount"] = _series_or_empty(pairs, "Amount_wso")
    summary["tr_gva_value_date"] = _series_or_empty(pairs, "tr_value_date_gva")
    summary["tr_wso_value_date"] = _series_or_empty(pairs, "tr_value_date_wso")
    summary["tr_gva_account"] = _series_or_empty(pairs, "Set ID_gva")
    summary["tr_gva_ex_id"] = _series_or_empty(pairs, "Exception ID_gva")
    summary["tr_wso_comm"] = _series_or_empty(pairs, "Last note Exception_wso")
    return summary[SUMMARY_COLUMNS]


def _run_tier_a(dataset: pd.DataFrame, tier: TierDefinition) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    result = dataset.copy(deep=False)

    active_mask = (
        (~result["tr_found"]) &
        result["tr_issuer_name"].astype("string").str.strip().ne("") &
        result["tr_rec_name"].isin(["GVA", "WSO"])
    )
    active = result.loc[active_mask].copy()
    if active.empty:
        return result, _empty_summary(), 0

    active["_tier_key"] = _build_exact_key(active, include_issuer=True)

    gva = active[active["tr_rec_name"] == "GVA"]
    wso = active[active["tr_rec_name"] == "WSO"]
    if gva.empty or wso.empty:
        return result, _empty_summary(), 0

    pairs = _pair_by_key(gva, wso, "_tier_key")
    if pairs.empty:
        return result, _empty_summary(), 0

    matched_idx = pd.Index(pairs["_row_index_gva"]).append(pd.Index(pairs["_row_index_wso"]))
    result.loc[matched_idx, "tr_found"] = True

    summary = _build_tier_a_summary(pairs, tier)
    return result, summary, len(summary)


def _run_tier_b(dataset: pd.DataFrame, tier: TierDefinition) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    result = dataset.copy(deep=False)

    active_mask = (~result["tr_found"]) & result["tr_rec_name"].isin(["GVA", "WSO"])
    active = result.loc[active_mask].copy()
    if active.empty:
        return result, _empty_summary(), 0

    active["_base_key"] = _build_exact_key(active, include_issuer=False)
    active["_issuer_norm"] = active["tr_issuer_name"].astype("string").str.strip()

    gva = active[active["tr_rec_name"] == "GVA"]
    wso = active[active["tr_rec_name"] == "WSO"]
    if gva.empty or wso.empty:
        return result, _empty_summary(), 0

    # Stage 1: exact issuer match for non-empty issuers.
    gva_non_empty = gva[gva["_issuer_norm"].ne("")].copy()
    wso_non_empty = wso[wso["_issuer_norm"].ne("")].copy()
    gva_non_empty["_issuer_key"] = gva_non_empty["_base_key"] + "|" + gva_non_empty["_issuer_norm"]
    wso_non_empty["_issuer_key"] = wso_non_empty["_base_key"] + "|" + wso_non_empty["_issuer_norm"]
    pairs_same_issuer = _pair_by_key(gva_non_empty, wso_non_empty, "_issuer_key")

    gva_left = _drop_matched(gva, pairs_same_issuer, "_row_index_gva")
    wso_left = _drop_matched(wso, pairs_same_issuer, "_row_index_wso")

    # Stage 2: allow blank issuer on GVA side matched with any remaining WSO by base key.
    gva_blank = gva_left[gva_left["_issuer_norm"].eq("")]
    pairs_gva_blank = _pair_by_key(gva_blank, wso_left, "_base_key")

    gva_left = _drop_matched(gva_left, pairs_gva_blank, "_row_index_gva")
    wso_left = _drop_matched(wso_left, pairs_gva_blank, "_row_index_wso")

    # Stage 3: allow blank issuer on WSO side matched with remaining non-blank GVA by base key.
    gva_non_blank_left = gva_left[gva_left["_issuer_norm"].ne("")]
    wso_blank_left = wso_left[wso_left["_issuer_norm"].eq("")]
    pairs_wso_blank = _pair_by_key(gva_non_blank_left, wso_blank_left, "_base_key")

    pair_frames = [df for df in [pairs_same_issuer, pairs_gva_blank, pairs_wso_blank] if not df.empty]
    if not pair_frames:
        return result, _empty_summary(), 0

    pairs = pd.concat(pair_frames, ignore_index=True)
    pairs = pairs.sort_values(by=["_base_key", "_row_index_gva", "_row_index_wso"], kind="stable")

    matched_idx = pd.Index(pairs["_row_index_gva"]).append(pd.Index(pairs["_row_index_wso"]))
    result.loc[matched_idx, "tr_found"] = True

    summary = _build_tier_a_summary(pairs, tier)
    return result, summary, len(summary)


def _build_tier_plan() -> list[TierDefinition]:
    return [
        TierDefinition(label="A", explanation="Exact Match", enabled=True, runner=_run_tier_a),
        TierDefinition(label="B", explanation="exact match (except issuer)", enabled=True, runner=_run_tier_b),
    ]


def run_tiers_pipeline(dataset: pd.DataFrame) -> TiersResult:
    result = dataset.copy(deep=False)
    result["tr_found"] = result["tr_found"].fillna(False).astype(bool)

    tier_counts: dict[str, int] = {}
    summary_frames: list[pd.DataFrame] = []

    for tier in _build_tier_plan():
        if not tier.enabled:
            continue
        result, summary_rows, match_count = tier.runner(result, tier)
        tier_counts[tier.label] = match_count
        if not summary_rows.empty:
            summary_frames.append(summary_rows)

    summary = pd.concat(summary_frames, ignore_index=True) if summary_frames else _empty_summary()

    return TiersResult(
        dataset=result,
        summary=summary,
        tier_counts=tier_counts,
        total_matches=int(sum(tier_counts.values())),
    )
