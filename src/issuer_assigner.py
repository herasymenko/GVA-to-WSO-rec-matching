from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Protocol

import pandas as pd


class HasIssuerColumns(Protocol):
    key_issuer_col: str
    key_value_col: str


@dataclass
class IssuerAssignResult:
    dataset: pd.DataFrame
    issuer_assigned_count: int


@dataclass
class _AhoAutomaton:
    transitions: list[dict[str, int]]
    fail: list[int]
    outputs: list[list[str]]


def _is_better_key(candidate: str, current: str) -> bool:
    if not current:
        return True
    if len(candidate) != len(current):
        return len(candidate) > len(current)
    return candidate < current


def _build_aho_automaton(keys: list[str]) -> _AhoAutomaton:
    transitions: list[dict[str, int]] = [{}]
    fail: list[int] = [0]
    outputs: list[list[str]] = [[]]

    for key in keys:
        state = 0
        for char in key:
            next_state = transitions[state].get(char)
            if next_state is None:
                next_state = len(transitions)
                transitions[state][char] = next_state
                transitions.append({})
                fail.append(0)
                outputs.append([])
            state = next_state
        outputs[state].append(key)

    queue: deque[int] = deque()
    for _, state in transitions[0].items():
        queue.append(state)
        fail[state] = 0

    while queue:
        state = queue.popleft()
        for char, next_state in transitions[state].items():
            queue.append(next_state)

            fallback = fail[state]
            while fallback and char not in transitions[fallback]:
                fallback = fail[fallback]

            fail[next_state] = transitions[fallback].get(char, 0)
            outputs[next_state].extend(outputs[fail[next_state]])

    return _AhoAutomaton(transitions=transitions, fail=fail, outputs=outputs)


def _find_best_key(text: str, automaton: _AhoAutomaton) -> str:
    state = 0
    best_key = ""

    for char in text:
        while state and char not in automaton.transitions[state]:
            state = automaton.fail[state]

        state = automaton.transitions[state].get(char, 0)
        if not automaton.outputs[state]:
            continue

        for matched_key in automaton.outputs[state]:
            if _is_better_key(matched_key, best_key):
                best_key = matched_key

    return best_key


def assign_issuers(dataset: pd.DataFrame, key_values_df: pd.DataFrame, loaded: HasIssuerColumns) -> IssuerAssignResult:
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

    automaton = _build_aho_automaton(list(key_to_issuer.keys()))
    ref_series = result["tr_ref_data"].astype("string").fillna("").str.casefold()
    matched_issuer: list[str] = []
    matched_key: list[str] = []

    for text in ref_series:
        issuer_key = _find_best_key(str(text), automaton)
        issuer_name = key_to_issuer.get(issuer_key, "")
        matched_issuer.append(issuer_name)
        matched_key.append(issuer_key)

    result["tr_issuer_name"] = matched_issuer
    result["tr_issuer_keys"] = matched_key

    assigned_count = int(pd.Series(matched_issuer).astype("string").str.len().gt(0).sum())
    return IssuerAssignResult(dataset=result, issuer_assigned_count=assigned_count)
