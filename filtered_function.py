import pandas as pd
import numpy as np
from sqlalchemy import text
from db_module.connect_sqlalchemy_engine import DBConnectionManager

# ======== DB 연결 ======== #
engine = DBConnectionManager.get_sync_engine()


def run_conditional_lateral_backtest(
    symbol: str,
    interval: str,
    strategy_sql: str,
    risk_reward_ratio: float,
    stop_loss_type: str = "low",
    stop_loss_value: float | None = None,
    start_time: str = None,
    end_time: str = None,
) -> pd.DataFrame:
    """
    trading_data 스키마 기반 백테스트
    - OHLCV + Indicators 테이블 조인
    - stop_loss_type='low' → e.low 기준
    - stop_loss_type='custom' → 사용자가 지정한 stop_loss_value 고정
    """

    # --- 테이블 이름 ---
    ohlcv_table = f"trading_data.ohlcv_{interval}".lower()
    indi_table = f"trading_data.indicators_{interval}".lower()

    # --- 사용된 보조지표 감지 ---
    groups = [
        ("rsi_14", ["rsi_14"]),
        ("ema_7", ["ema_7"]),
        ("ema_21", ["ema_21"]),
        ("ema_99", ["ema_99"]),
        ("macd", ["macd", "macd_signal"]),
        ("boll", ["bb_upper", "bb_middle", "bb_lower"]),
    ]
    used_indicators = [
        name for name, keywords in groups if any(k in strategy_sql for k in keywords)
    ]
    what_indicators_str = (
        " and ".join(sorted(used_indicators)) if used_indicators else "None"
    )

    # --- 손절가 설정 ---
    if stop_loss_type == "low":
        stop_loss_expr = "e.low"
    else:
        # custom stop_loss
        stop_loss_expr = str(stop_loss_value or 0.0)

    # --- 기간 필터 SQL ---
    time_conditions = []
    if start_time:
        time_conditions.append(f"o.timestamp >= '{start_time}'")
    if end_time:
        time_conditions.append(f"o.timestamp <= '{end_time}'")
    time_filter_sql = " AND " + " AND ".join(time_conditions) if time_conditions else ""

    # --- 핵심 쿼리 ---
    query = f"""
    SELECT
        e.timestamp AS entry_time,
        e.close AS entry_price,
        ({stop_loss_expr}) AS stop_loss,
        e.close + (e.close - ({stop_loss_expr})) * :rr_ratio AS take_profit,
        x.timestamp AS exit_time,
        CASE
            WHEN x.timestamp IS NULL THEN 'OPEN'
            WHEN x.low <= ({stop_loss_expr}) THEN 'SL'
            WHEN x.high >= (e.close + (e.close - ({stop_loss_expr})) * :rr_ratio) THEN 'TP'
            ELSE 'UNKNOWN'
        END AS result,
        :symbol AS symbol,
        :interval AS interval,
        '{strategy_sql}' AS strategy,
        '{what_indicators_str}' AS what_indicators
    FROM (
        SELECT o.timestamp, o.close, o.low
        FROM {ohlcv_table} AS o
        JOIN {indi_table} AS i USING (symbol, timestamp)
        WHERE {strategy_sql}
        {time_filter_sql}
    ) e
    LEFT JOIN LATERAL (
        SELECT x.timestamp, x.low, x.high
        FROM {ohlcv_table} AS x
        WHERE x.timestamp > e.timestamp
          AND (
              x.low <= ({stop_loss_expr})
              OR x.high >= (e.close + (e.close - ({stop_loss_expr})) * :rr_ratio)
          )
        ORDER BY x.timestamp
        LIMIT 1
    ) x ON TRUE;
    """

    # --- 쿼리 실행 ---
    with engine.connect() as conn:
        df = pd.read_sql(
            text(query),
            conn,
            params={
                "rr_ratio": risk_reward_ratio,
                "symbol": symbol,
                "interval": interval,
            },
        )

    if df.empty:
        return df

    # --- 수익률 계산 ---
    df["profit_rate"] = 0.0
    non_zero = df["entry_price"] != 0
    df.loc[(df["result"] == "TP") & non_zero, "profit_rate"] = (
        df["take_profit"] - df["entry_price"]
    ) / df["entry_price"]
    df.loc[(df["result"] == "SL") & non_zero, "profit_rate"] = (
        df["stop_loss"] - df["entry_price"]
    ) / df["entry_price"]

    # --- 중복 제거 (entry_time + exit_time 기준) ---
    df = df.drop_duplicates(subset=["entry_time", "exit_time"], keep="first")

    # --- 복리 누적 수익률 계산 ---
    df = df.sort_values("entry_time").reset_index(drop=True)
    cum_return = 1.0
    cum_profit_list = []
    for pr in df["profit_rate"]:
        cum_return *= 1 + pr
        cum_profit_list.append(cum_return - 1)
    df["cum_profit_rate"] = np.array(cum_profit_list) * 100
    df["profit_rate"] *= 100

    return df


# ============================
# ✅ 결과 저장
# ============================
def save_result_to_table(data: pd.DataFrame):
    """
    trading_data.filtered 테이블에 결과 저장
    """
    if data.empty:
        print("⚠️ 저장할 결과가 없습니다.")
        return

    table_name = "trading_data.filtered"
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        entry_time TIMESTAMPTZ PRIMARY KEY,
        entry_price DOUBLE PRECISION,
        stop_loss DOUBLE PRECISION,
        take_profit DOUBLE PRECISION,
        exit_time TIMESTAMPTZ,
        result TEXT,
        symbol TEXT,
        interval TEXT,
        strategy TEXT,
        what_indicators TEXT,
        profit_rate DOUBLE PRECISION,
        cum_profit_rate DOUBLE PRECISION
    );
    """

    with engine.begin() as conn:
        conn.execute(text(create_table_query))
        conn.execute(text(f"DELETE FROM {table_name}"))

    # DB에 저장
    data.to_sql(
        name="filtered",
        schema="trading_data",
        con=engine,
        if_exists="append",
        index=False,
    )
    print("✅ trading_data.filtered 테이블에 결과 저장 완료")


# ============================
# ✅ 통계 계산
# ============================
def calculate_statics() -> dict:
    """
    trading_data.filtered 테이블 기반 TP/SL 통계 계산
    """
    query = "SELECT * FROM trading_data.filtered"
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    if df.empty:
        return {
            "total_count": 0,
            "tp_count": 0,
            "sl_count": 0,
            "tp_rate": 0.0,
            "expectancy": 0.0,
            "final_profit_rate": 0.0,
        }

    # 기본 통계
    total_count = df["result"].isin(["TP", "SL"]).sum()
    tp_count = (df["result"] == "TP").sum()
    sl_count = (df["result"] == "SL").sum()
    tp_rate = round(tp_count * 100 / total_count, 2) if total_count else 0.0

    # 기대수익률 계산
    df_profit = df[df["result"] == "TP"]["profit_rate"]
    df_loss = df[df["result"] == "SL"]["profit_rate"]
    expectancy = (
        (tp_count * df_profit.mean() + sl_count * df_loss.mean()) / total_count
        if total_count
        else 0.0
    )

    return {
        "total_count": int(total_count),
        "tp_count": int(tp_count),
        "sl_count": int(sl_count),
        "tp_rate": float(tp_rate),
        "expectancy": float(expectancy),
        "final_profit_rate": float(df["cum_profit_rate"].iloc[-1]),
    }
