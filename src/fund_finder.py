from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from loader import LoadedInputs
from normalize import NormalizeResult
from schema_errors import SchemaError


@dataclass
class FundFinderResult:
    dataset: pd.DataFrame
    fund_not_found: pd.DataFrame
    coverage_found: int
    coverage_not_found: int


def apply_fund_mapping(normalized: NormalizeResult, loaded: LoadedInputs) -> FundFinderResult:
    dataset = normalized.normalized_df.copy()

    mapping = loaded.mapping_df[[loaded.mapping_set_id_col, loaded.mapping_fund_col]].copy()
    mapping[loaded.mapping_set_id_col] = mapping[loaded.mapping_set_id_col].astype("string").str.strip()
    mapping[loaded.mapping_fund_col] = mapping[loaded.mapping_fund_col].astype("string").str.strip()
    mapping = mapping.dropna(subset=[loaded.mapping_set_id_col, loaded.mapping_fund_col])

    duplicates = mapping[mapping.duplicated(subset=[loaded.mapping_set_id_col], keep=False)]
    if not duplicates.empty:
        raise SchemaError(
            code="FUND_FINDER_MAPPING_CONFLICT",
            message="Mapping table contains duplicate Set ID keys",
            hint="Set ID in mapping table must be unique.",
        )

    mapping_dict = dict(zip(mapping[loaded.mapping_set_id_col], mapping[loaded.mapping_fund_col]))

    set_id_series = dataset["Set ID"].astype("string").str.strip()
    dataset["tr_fund_name"] = set_id_series.map(mapping_dict).fillna("")

    missing_mask = dataset["tr_fund_name"].astype("string").str.strip().eq("")
    fund_not_found = dataset[missing_mask].copy()

    return FundFinderResult(
        dataset=dataset,
        fund_not_found=fund_not_found,
        coverage_found=int((~missing_mask).sum()),
        coverage_not_found=int(missing_mask.sum()),
    )
