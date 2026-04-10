from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from loader import LoadedMappingInputs
from normalize import NormalizeResult
from schema_errors import SchemaError


@dataclass
class FundFinderResult:
    dataset: pd.DataFrame
    fund_not_found: pd.DataFrame
    coverage_found: int
    coverage_not_found: int


def apply_fund_mapping(normalized: NormalizeResult, loaded: LoadedMappingInputs) -> FundFinderResult:
    dataset = normalized.normalized_df.copy(deep=False)

    def build_mapping(mapping_df: pd.DataFrame, set_id_col: str, conflict_code: str) -> dict[str, str]:
        mapping = mapping_df[[set_id_col, loaded.mapping_fund_col]].copy()
        mapping[set_id_col] = mapping[set_id_col].astype("string").str.strip()
        mapping[loaded.mapping_fund_col] = mapping[loaded.mapping_fund_col].astype("string").str.strip()
        mapping = mapping.dropna(subset=[set_id_col, loaded.mapping_fund_col])
        duplicates = mapping[mapping.duplicated(subset=[set_id_col], keep=False)]
        if not duplicates.empty:
            raise SchemaError(
                code=conflict_code,
                message="Mapping table contains duplicate Set ID keys",
                hint="Set ID in each mapping sheet must be unique.",
            )
        return dict(zip(mapping[set_id_col], mapping[loaded.mapping_fund_col]))

    wso_mapping = build_mapping(
        loaded.wso_mapping_df,
        loaded.wso_mapping_set_id_col,
        "FUND_FINDER_WSO_MAPPING_CONFLICT",
    )
    gva_mapping = build_mapping(
        loaded.gva_mapping_df,
        loaded.gva_mapping_set_id_col,
        "FUND_FINDER_GVA_MAPPING_CONFLICT",
    )

    dataset["tr_fund_name"] = ""
    set_id_series = dataset["Set ID"].astype("string").str.strip()

    wso_mask = dataset["tr_rec_name"] == "WSO"
    gva_mask = dataset["tr_rec_name"] == "GVA"

    dataset.loc[wso_mask, "tr_fund_name"] = set_id_series[wso_mask].map(wso_mapping).fillna("")
    dataset.loc[gva_mask, "tr_fund_name"] = set_id_series[gva_mask].map(gva_mapping).fillna("")

    missing_mask = dataset["tr_fund_name"].astype("string").str.strip().eq("")
    fund_not_found = dataset[missing_mask].copy()
    dataset = dataset[~missing_mask]

    return FundFinderResult(
        dataset=dataset,
        fund_not_found=fund_not_found,
        coverage_found=len(dataset),
        coverage_not_found=len(fund_not_found),
    )
