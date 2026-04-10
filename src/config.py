from __future__ import annotations

TR_COLUMNS = [
    "tr_found",
    "tr_rec_name",
    "tr_fund_name",
    "tr_issuer_name",
    "tr_issuer_keys",
    "tr_entry_type",
    "tr_side",
    "tr_currency",
    "tr_amount_cents",
    "tr_value_date",
    "tr_ref_data",
    "tr_item_id",
]

REQUIRED_SOURCE_FIELDS = {
    "set_id": "Set ID",
    "entry_type": "Entry Type",
    "side": "Side",
    "currency": "Currency",
    "amount": "Amount",
    "value_date": "Value Date",
    "item_id": "item id",
}

SOURCE_ALIASES = {
    "Set ID": ["Set ID"],
    "Entry Type": ["Entry Type"],
    "Side": ["Side"],
    "Currency": ["Currency"],
    "Amount": ["Amount"],
    "Value Date": ["Value Date"],
    "item id": ["item id", "Item ID", "item_id"],
    "Ref1": ["Ref1", "Ref 1"],
    "Ref2": ["Ref2", "Ref 2"],
    "Ref3": ["Ref3", "Ref 3"],
    "Ref4": ["Ref4", "Ref 4"],
    "Original ID": [
        "Original ID",
        "\u00d3riginal ID",
        "O\u0304riginal ID",
    ],
    "Asset Desc": ["Asset Desc", "Asset Description"],
}

REFERENCE_SOURCE_FIELDS = [
    "Ref1",
    "Ref2",
    "Ref3",
    "Ref4",
    "Original ID",
    "Asset Desc",
]

SIDE_MAP = {
    "buy": "B",
    "b": "B",
    "sell": "S",
    "s": "S",
    "debit": "D",
    "d": "D",
    "credit": "C",
    "c": "C",
    "long": "L",
    "l": "L",
    "short": "SH",
    "sh": "SH",
}

OUTPUT_FILE_NAME = "gva_wso_matches_uniquenumber.xlsx"
SHEET_GVA_WSO = "gva_wso"
SHEET_FUND_NOT_FOUND = "fund_not_found"
SHEET_SUMMARY = "gva_wso_summary"

MAPPING_SET_ID_COL_CANDIDATES = ["Set ID WSO", "Set ID", "set_id_wso"]
MAPPING_FUND_COL_CANDIDATES = ["Fund Name", "fund_name", "Fund"]
MAPPING_WSO_SHEET_NAME = "WSO"
MAPPING_GVA_SHEET_NAME = "GVA"
MAPPING_WSO_SET_ID_COL_CANDIDATES = ["Set ID WSO", "Set ID", "set_id_wso"]
MAPPING_GVA_SET_ID_COL_CANDIDATES = ["Set ID GVA", "Set ID", "set_id_gva"]
KEY_VALUES_ISSUER_COL_CANDIDATES = ["issuer_name", "Issuer", "issuer"]
KEY_VALUES_KEY_COL_CANDIDATES = ["key", "issuer_key", "Key"]
