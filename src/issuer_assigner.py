from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from loader import LoadedInputs


@dataclass
class IssuerAssignResult:
    dataset: pd.DataFrame
    issuer_assigned_count: int


def assign_issuers(dataset: pd.DataFrame, key_values_df: pd.DataFrame, loaded: LoadedInputs) -> IssuerAssignResult:
    result = dataset.copy()
    issuer_col = loaded.key_issuer_col
    key_col = loaded.key_value_col

    key_values = key_values_df[[issuer_col, key_col]].dropna().copy()
    key_values[issuer_col] = key_values[issuer_col].astype("string").str.casefold().str.strip()
    key_values[key_col] = key_values[key_col].astype("string").str.casefold().str.strip()
    key_values = key_values[key_values[key_col].str.len() > 0]

    key_to_issuer: dict[str, str] = {}
    for _, row in key_values.iterrows():
        key = str(row[key_col])
        issuer = str(row[issuer_col])
        if key not in key_to_issuer:
            key_to_issuer[key] = issuer

    ref_series = result["tr_ref_data"].astype("string").str.casefold()
    matched_issuer: list[str] = []
    matched_key: list[str] = []

    sorted_keys = sorted(key_to_issuer.keys(), key=lambda k: (-len(k), k))
    for text in ref_series:
        issuer_name = ""
        issuer_key = ""
        for key in sorted_keys:
            if key and key in text:
                issuer_key = key
                issuer_name = key_to_issuer[key]
                break
        matched_issuer.append(issuer_name)
        matched_key.append(issuer_key)

    result["tr_issuer_name"] = matched_issuer
    result["tr_issuer_keys"] = matched_key

    assigned_count = int(pd.Series(matched_issuer).astype("string").str.len().gt(0).sum())
    return IssuerAssignResult(dataset=result, issuer_assigned_count=assigned_count)
