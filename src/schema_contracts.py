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


# Canonical source fields required to proceed.
REQUIRED_SOURCE_FIELDS = {
    "set_id": "Set ID",
    "entry_type": "Entry Type",
    "side": "Side",
    "currency": "Currency",
    "amount": "Amount",
    "value_date": "Value Date",
    "item_id": "item id",
}


# Known aliases only, as agreed.
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
