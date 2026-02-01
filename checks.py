from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

import pandas as pd


@dataclass
class QCResult:
    check_name: str
    passed: bool
    detail: str


def check_min_rows(df: pd.DataFrame, min_rows: int, name: str = "min_rows") -> QCResult:
    passed = len(df) >= min_rows
    detail = f"rows={len(df)} min_rows={min_rows}"
    return QCResult(name, passed, detail)


def check_non_null(df: pd.DataFrame, col: str, max_null_rate: float = 0.01, name: str = "non_null") -> QCResult:
    if col not in df.columns:
        return QCResult(name, False, f"missing_column={col}")
    null_rate = df[col].isna().mean()
    passed = null_rate <= max_null_rate
    detail = f"null_rate={null_rate:.4f} max_null_rate={max_null_rate}"
    return QCResult(f"{name}:{col}", passed, detail)


def run_checks(df: pd.DataFrame, checks: List[Callable[[pd.DataFrame], QCResult]]) -> List[QCResult]:
    results = []
    for fn in checks:
        results.append(fn(df))
    return results
