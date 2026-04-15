from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Callable

import pandas as pd

from config import (
    SUMMARY_COLUMNS,
    TIER_C_LINEAR_END_AMOUNT_CENTS,
    TIER_C_LINEAR_START_AMOUNT_CENTS,
    TIER_C_MAX_TOLERANCE_CENTS,
    TIER_C_MIN_TOLERANCE_CENTS,
    TIER_D_MAX_DATE_DIFF_DAYS,
    TR_WSO_COMM_CODE_RULES,
    TR_WSO_COMM_DEFAULT_COMMENT,
    TR_WSO_COMM_FORMAT_PREFIX,
    TR_WSO_COMM_STRIP_PREFIX_REGEX,
)


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


def _normalize_wso_comments(summary: pd.DataFrame) -> pd.DataFrame:
    if summary.empty or "tr_wso_comm" not in summary.columns:
        return summary

    result = summary.copy(deep=False)
    raw = result["tr_wso_comm"].astype("string").fillna("")
    cleaned = raw.str.replace(TR_WSO_COMM_STRIP_PREFIX_REGEX, "", regex=True).str.strip()

    default_code = "[MO]"
    code_series = pd.Series(default_code, index=result.index, dtype="string")

    for code, substrings in TR_WSO_COMM_CODE_RULES:
        if code == default_code or not substrings:
            continue

        mask = pd.Series(False, index=result.index)
        for substring in substrings:
            pattern = re.escape(substring)
            mask = mask | cleaned.str.contains(pattern, case=False, na=False, regex=True)

        code_series = code_series.where(~(mask & code_series.eq(default_code)), code)

    comment_text = cleaned.where(cleaned.ne(""), TR_WSO_COMM_DEFAULT_COMMENT)

    issuer_text = result["tr_issuer_name"].astype("string").fillna("").str.strip()

    base = code_series + " " + TR_WSO_COMM_FORMAT_PREFIX + comment_text
    with_issuer = base + " (" + issuer_text + ")"
    result["tr_wso_comm"] = with_issuer.where(issuer_text.ne(""), base)
    return result


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
        gva_issuer = _series_or_empty(pairs, "tr_issuer_name_gva").astype("string").fillna("").str.strip()
        wso_issuer = _series_or_empty(pairs, "tr_issuer_name_wso").astype("string").fillna("").str.strip()
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


def _tier_c_tolerance(max_amount_cents: pd.Series) -> pd.Series:
    # Piecewise tolerance in cents, configured via config.py constants.
    max_amount = pd.to_numeric(max_amount_cents, errors="coerce").fillna(0).astype("int64")
    tolerance = pd.Series(TIER_C_MAX_TOLERANCE_CENTS, index=max_amount.index, dtype="int64")

    lt_start = max_amount < TIER_C_LINEAR_START_AMOUNT_CENTS
    mid = (max_amount >= TIER_C_LINEAR_START_AMOUNT_CENTS) & (max_amount < TIER_C_LINEAR_END_AMOUNT_CENTS)

    tolerance.loc[lt_start] = TIER_C_MIN_TOLERANCE_CENTS
    span_amount = max(1, TIER_C_LINEAR_END_AMOUNT_CENTS - TIER_C_LINEAR_START_AMOUNT_CENTS)
    span_tolerance = TIER_C_MAX_TOLERANCE_CENTS - TIER_C_MIN_TOLERANCE_CENTS
    tolerance.loc[mid] = (
        TIER_C_MIN_TOLERANCE_CENTS +
        ((max_amount.loc[mid] - TIER_C_LINEAR_START_AMOUNT_CENTS) * span_tolerance) // span_amount
    ).astype("int64")
    return tolerance


def _run_tier_c(dataset: pd.DataFrame, tier: TierDefinition) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    result = dataset.copy(deep=False)

    active_mask = (
        (~result["tr_found"]) &
        result["tr_rec_name"].isin(["GVA", "WSO"]) &
        result["tr_currency"].astype("string").str.strip().ne("") &
        result["tr_fund_name"].astype("string").str.strip().ne("") &
        result["tr_entry_type"].astype("string").str.strip().ne("")
    )
    active = result.loc[active_mask].copy()
    if active.empty:
        return result, _empty_summary(), 0

    issuer_norm = active["tr_issuer_name"].astype("string").fillna("").str.strip()
    active["_tier_c_key"] = (
        active["tr_fund_name"].astype("string").fillna("") + "|" +
        issuer_norm + "|" +
        active["tr_side"].astype("string").fillna("") + "|" +
        active["tr_currency"].astype("string").fillna("") + "|" +
        active["tr_entry_type"].astype("string").fillna("") + "|" +
        active["tr_value_date"].astype("string").fillna("")
    )

    gva = active[active["tr_rec_name"] == "GVA"]
    wso = active[active["tr_rec_name"] == "WSO"]
    if gva.empty or wso.empty:
        return result, _empty_summary(), 0

    left = gva.sort_index().copy()
    right = wso.sort_index().copy()

    left["_pair_seq"] = left.groupby("_tier_c_key", sort=False).cumcount()
    right["_pair_seq"] = right.groupby("_tier_c_key", sort=False).cumcount()
    left["_row_index_gva"] = left.index
    right["_row_index_wso"] = right.index

    pairs = left.merge(
        right,
        how="inner",
        on=["_tier_c_key", "_pair_seq"],
        suffixes=("_gva", "_wso"),
        sort=False,
    )
    if pairs.empty:
        return result, _empty_summary(), 0

    gva_amount = pd.to_numeric(pairs["tr_amount_cents_gva"], errors="coerce")
    wso_amount = pd.to_numeric(pairs["tr_amount_cents_wso"], errors="coerce")
    diff_amount = (gva_amount - wso_amount).abs()
    max_amount = pd.concat([gva_amount.abs(), wso_amount.abs()], axis=1).max(axis=1)
    tolerance = _tier_c_tolerance(max_amount)

    within_tolerance = diff_amount.le(tolerance)
    pairs = pairs.loc[within_tolerance]
    if pairs.empty:
        return result, _empty_summary(), 0

    pairs = pairs.sort_values(
        by=["_tier_c_key", "_pair_seq", "_row_index_gva", "_row_index_wso"],
        kind="stable",
    )

    matched_idx = pd.Index(pairs["_row_index_gva"]).append(pd.Index(pairs["_row_index_wso"]))
    result.loc[matched_idx, "tr_found"] = True

    summary = _build_tier_a_summary(pairs, tier)
    return result, summary, len(summary)


def _run_tier_d(dataset: pd.DataFrame, tier: TierDefinition) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    result = dataset.copy(deep=False)

    active_mask = (
        (~result["tr_found"]) &
        result["tr_rec_name"].isin(["GVA", "WSO"]) &
        result["tr_fund_name"].astype("string").str.strip().ne("") &
        result["tr_issuer_name"].astype("string").str.strip().ne("") &
        result["tr_entry_type"].astype("string").str.strip().ne("") &
        result["tr_currency"].astype("string").str.strip().ne("")
    )
    active = result.loc[active_mask].copy()
    if active.empty:
        return result, _empty_summary(), 0

    amount_norm = pd.to_numeric(active["tr_amount_cents"], errors="coerce")
    active = active.loc[amount_norm.notna()].copy()
    if active.empty:
        return result, _empty_summary(), 0
    active["_amount_norm"] = amount_norm.loc[active.index].astype("int64")

    active["_tier_d_key"] = (
        active["tr_fund_name"].astype("string").fillna("") + "|" +
        active["tr_issuer_name"].astype("string").fillna("").str.strip() + "|" +
        active["_amount_norm"].astype("string") + "|" +
        active["tr_entry_type"].astype("string").fillna("") + "|" +
        active["tr_currency"].astype("string").fillna("")
    )

    gva = active[active["tr_rec_name"] == "GVA"]
    wso = active[active["tr_rec_name"] == "WSO"]
    if gva.empty or wso.empty:
        return result, _empty_summary(), 0

    left = gva.sort_index().copy()
    right = wso.sort_index().copy()
    left["_row_index_gva"] = left.index
    right["_row_index_wso"] = right.index
    left["_value_date_dt"] = pd.to_datetime(left["tr_value_date"], errors="coerce")
    right["_value_date_dt"] = pd.to_datetime(right["tr_value_date"], errors="coerce")

    left = left[left["_value_date_dt"].notna()]
    right = right[right["_value_date_dt"].notna()]
    if left.empty or right.empty:
        return result, _empty_summary(), 0

    pairs = left.merge(
        right,
        how="inner",
        on=["_tier_d_key"],
        suffixes=("_gva", "_wso"),
        sort=False,
    )
    if pairs.empty:
        return result, _empty_summary(), 0

    date_diff_days = (pairs["_value_date_dt_gva"] - pairs["_value_date_dt_wso"]).abs().dt.days
    pairs = pairs.loc[date_diff_days.le(TIER_D_MAX_DATE_DIFF_DAYS)].copy()
    if pairs.empty:
        return result, _empty_summary(), 0
    pairs["_date_diff_days"] = date_diff_days.loc[pairs.index]

    # Greedy one-to-one matching: closest date first, then stable by source row index.
    pairs = pairs.sort_values(
        by=["_tier_d_key", "_date_diff_days", "_row_index_gva", "_row_index_wso"],
        kind="stable",
    )
    pairs = pairs.loc[~pairs["_row_index_gva"].duplicated(keep="first")]
    pairs = pairs.loc[~pairs["_row_index_wso"].duplicated(keep="first")]
    if pairs.empty:
        return result, _empty_summary(), 0

    matched_idx = pd.Index(pairs["_row_index_gva"]).append(pd.Index(pairs["_row_index_wso"]))
    result.loc[matched_idx, "tr_found"] = True

    summary = _build_tier_a_summary(pairs, tier)
    return result, summary, len(summary)


def _build_tier_plan() -> list[TierDefinition]:
    return [
        TierDefinition(label="A", explanation="Exact match. Issuer found", enabled=True, runner=_run_tier_a),
        TierDefinition(label="B", explanation="Exact match. Issuer not found", enabled=True, runner=_run_tier_b),
        TierDefinition(label="C", explanation="Amount diff, low tolerance. Issuer found", enabled=True, runner=_run_tier_c),
        TierDefinition(label="D", explanation="Date diff. Issuer found", enabled=True, runner=_run_tier_d),
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
    summary = _normalize_wso_comments(summary)

    return TiersResult(
        dataset=result,
        summary=summary,
        tier_counts=tier_counts,
        total_matches=int(sum(tier_counts.values())),
    )
