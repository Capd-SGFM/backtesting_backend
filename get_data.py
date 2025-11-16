import pandas as pd
import numpy as np
from binance_client import fetch_klines
from datetime import datetime, timezone  # 시간 변환용
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

def insert_ohlcv_rows(schema: str, table_name: str, rows: list[dict]) -> None:
    """
    OHLCV dict 리스트를 해당 테이블에 INSERT
    rows 예시:
    {
        "timestamp": datetime,
        "open": 123.4,
        "high": 125.0,
        "low": 120.0,
        "close": 124.5,
        "volume": 1000.0,
    }
    """
    if not rows:
        print("⚠️ 삽입할 데이터가 없습니다.")
        return

    full_table = f"{schema}.{table_name}"

    sql = text(f"""
        INSERT INTO {full_table} ("timestamp", "open", "high", "low", "close", "volume")
        VALUES (:timestamp, :open, :high, :low, :close, :volume)
        ON CONFLICT ("timestamp") DO NOTHING;
        -- ↑ PK/UNIQUE 조합에 맞게 필요하면 수정
    """)

    with engine.begin() as conn:  # 자동 commit
        conn.execute(sql, rows)

    print(f"✅ {len(rows)} rows inserted into {full_table}")


def save_binance_ohlcv(
    symbol: str,
    interval: str = "1m",
    limit: int = 500,
    schema: str = "trading_data",
):
    """
    Binance USD-M 선물에서 OHLCV 불러와서
    trading_data.ohlcv_{interval} 테이블에 저장
    """
    # 1) Binance 에서 캔들 가져오기
    candles = fetch_klines(symbol, interval, limit)
    print(f"📥 Binance에서 가져온 캔들 수: {len(candles)}")

    # 2) DB에 맞는 형태로 변환
    rows: list[dict] = []
    for c in candles:
        # open_time(ms)를 Python datetime으로 변환
        ts = datetime.fromtimestamp(c["open_time"] / 1000, tz=timezone.utc)

        rows.append(
            {
                "timestamp": ts,
                "open": c["open"],
                "high": c["high"],
                "low": c["low"],
                "close": c["close"],
                "volume": c["volume"],
            }
        )

    table_name = f"ohlcv_{interval}".lower()

    # 3) INSERT 실행
    insert_ohlcv_rows(schema, table_name, rows)

if __name__ == "__main__":
    # 예시: BTCUSDT 1분봉 500개 저장
    save_binance_ohlcv("BTCUSDT", "1m", 500)