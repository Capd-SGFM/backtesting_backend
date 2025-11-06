import pandas as pd
import numpy as np
from sqlalchemy import text
from db_module.connect_sqlalchemy_engine import DBConnectionManager

engine = DBConnectionManager.get_sync_engine()


def wrap_strs_with_quote(x: str | list[str]) -> str:
    if isinstance(x, str):
        return f'"{x}"'
    return ", ".join([f'"{col}"' for col in x])


def get_data_from_table(
    schema: str,
    table_name: str,
    return_type: str | list[str],
    order_by: str | None = None,
    filter: str | None = None,
    min_value=None,
    max_value=None,
) -> list:
    """지정 스키마에서 테이블 데이터 조회"""
    full_table = f"{schema}.{table_name}"
    COLS = wrap_strs_with_quote(return_type)
    params = {}
    where_clause = ""

    if order_by is None:
        order_by = return_type if isinstance(return_type, str) else return_type[0]

    if filter is not None:
        where_clause = f"WHERE {filter} "
        if min_value is not None:
            params["min"] = min_value
        if max_value is not None:
            params["max"] = max_value

        match len(params):
            case 2:
                where_clause += "BETWEEN :min AND :max"
            case 1:
                where_clause += ">= :min" if "min" in params else "<= :max"
            case _:
                raise ValueError("WHERE field missing value(s)")

    query = text(
        f'SELECT {COLS} FROM {full_table} {where_clause} ORDER BY "{order_by}"'
    )

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params=params)

    if df.empty:
        return []

    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df.dropna(how="any", inplace=True)
    return df.to_dict(orient="records")


def get_ohlcv_data(symbol: str, interval: str, **kwargs) -> list:
    table_name = f"ohlcv_{interval}".lower()
    return_type = ["timestamp", "open", "high", "low", "close", "volume"]
    return get_data_from_table(
        schema="trading_data", table_name=table_name, return_type=return_type, **kwargs
    )


def get_filtered_data() -> list:
    return get_data_from_table(
        schema="trading_data",
        table_name="filtered",
        return_type=[
            "entry_time",
            "exit_time",
            "symbol",
            "interval",
            "entry_price",
            "stop_loss",
            "take_profit",
        ],
    )
