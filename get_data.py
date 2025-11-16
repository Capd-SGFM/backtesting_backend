import pandas as pd
import numpy as np
from sqlalchemy import text
from db_module.connect_sqlalchemy_engine import DBConnectionManager

# ✅ 동기 엔진 초기화
engine = DBConnectionManager.get_sync_engine()


# ==============================
# 문자열 포맷 도우미 ###
# ==============================
def wrap_strs_with_quote(x: str | list[str]) -> str:
    """컬럼명을 SQL용 큰따옴표로 감싸기"""
    if isinstance(x, str):
        return f'"{x}"'
    return ", ".join([f'"{col}"' for col in x])


# ==============================
# 범용 테이블 조회 함수
# ==============================
def get_data_from_table(
    schema: str,
    table_name: str,
    return_type: str | list[str],
    order_by: str | None = None,
    filter: str | None = None,
    min_value=None,
    max_value=None,
) -> list:
    """
    지정된 스키마/테이블에서 데이터를 조회.
    - schema: 스키마명 (예: trading_data)
    - table_name: 테이블명 (예: ohlcv_1h)
    - return_type: 반환할 컬럼명 리스트 또는 단일 문자열
    - order_by: 정렬 기준 컬럼 (기본값: 첫 번째 컬럼)
    - filter: WHERE 조건 필드
    - min_value, max_value: 필터링 값 (BETWEEN 등)
    """
    full_table = f"{schema}.{table_name}"
    COLS = wrap_strs_with_quote(return_type)
    params = {}
    where_clause = ""

    if order_by is None:
        order_by = return_type if isinstance(return_type, str) else return_type[0]

    # WHERE 절 구성
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

    # 쿼리 실행
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params=params)

    if df.empty:
        return []

    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df.dropna(how="any", inplace=True)
    return df.to_dict(orient="records")


# ==============================
# OHLCV 데이터 조회
# ==============================
def get_ohlcv_data(symbol: str, interval: str, **kwargs) -> list:
    """
    trading_data.ohlcv_{interval} 테이블에서 특정 심볼의 OHLCV 데이터 조회
    """
    table_name = f"ohlcv_{interval}".lower()
    return_type = ["timestamp", "open", "high", "low", "close", "volume"]
    return get_data_from_table(
        schema="trading_data", table_name=table_name, return_type=return_type, **kwargs
    )


# ==============================
# 백테스트 결과 데이터 조회 (users.backtest_results)
# ==============================
def get_filtered_data(google_id: str | None = None) -> list:
    """
    users.backtest_results 테이블에서 백테스트 결과 조회
    - google_id가 주어지면 해당 사용자 데이터만 조회
    """
    with engine.connect() as conn:
        if google_id:
            query = text(
                """
                SELECT 
                    google_id,
                    symbol,
                    interval,
                    strategy_sql,
                    risk_reward_ratio,
                    start_time,
                    end_time,
                    entry_time,
                    exit_time,
                    result,
                    profit_rate,
                    cum_profit_rate,
                    created_at,
                    updated_at
                FROM users.backtest_results
                WHERE google_id = :gid
                ORDER BY entry_time ASC;
            """
            )
            df = pd.read_sql(query, conn, params={"gid": google_id})
        else:
            query = text(
                """
                SELECT 
                    google_id,
                    symbol,
                    interval,
                    strategy_sql,
                    risk_reward_ratio,
                    start_time,
                    end_time,
                    entry_time,
                    exit_time,
                    result,
                    profit_rate,
                    cum_profit_rate,
                    created_at,
                    updated_at
                FROM users.backtest_results
                ORDER BY entry_time ASC;
            """
            )
            df = pd.read_sql(query, conn)

    if df.empty:
        return []

    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df.dropna(how="any", inplace=True)
    return df.to_dict(orient="records")
