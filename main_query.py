from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from sqlalchemy import text
from typing import Optional

from get_data import get_ohlcv_data, get_filtered_data, get_data_from_table
from filtered_function import (
    run_conditional_lateral_backtest,
    save_result_to_table,
    calculate_statics,
)
from db_module.connect_sqlalchemy_engine import DBConnectionManager
from auth_utils import verify_token, TokenData


# =========================
# 🔹 DB 엔진 연결 ###
# =========================
db_manager = DBConnectionManager()
engine = db_manager.get_sync_engine()


# =========================
# 🔹 FastAPI 앱 설정
# =========================
app = FastAPI(
    title="Backtesting Backend",
    description="백테스팅용 FastAPI 백엔드 (JWT 인증 + 손절가 커스터마이즈 + 중복 방지 + 누적 수익률 개선)",
    version="2.2.0",
)

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ⚠️ 실제 서비스 시에는 프론트엔드 도메인으로 제한 필요
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =========================
# 🔹 Request Body 정의
# =========================
class StrategyRequest(BaseModel):
    symbol: str
    interval: str
    strategy_sql: str
    risk_reward_ratio: float
    stop_loss_type: str = "low"
    stop_loss_value: Optional[float] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    position_side: str = "LONG"
    leverage: float = 1.0
    slippage_rate: float = 0.0


# =========================
# 1️⃣ 전략 실행 및 결과 저장
# =========================
@app.post("/save_strategy")
def save_strategy(
    req: StrategyRequest,
    token: TokenData = Depends(verify_token),
):
    """
    로그인된 사용자의 google_id, email, name 정보를 기반으로
    전략을 실행하고 결과를 users.backtest_results에 저장합니다.
    - 손절가 기준: 'low' 또는 사용자 지정값(custom)
    - 중복 entry_time 방지 (ON CONFLICT)
    """
    try:
        google_id = token.id
        username = token.name or (token.sub.split("@")[0] if token.sub else "unknown")

        # ✅ 전략 실행
        df = run_conditional_lateral_backtest(
            symbol=req.symbol,
            interval=req.interval,
            strategy_sql=req.strategy_sql,
            risk_reward_ratio=req.risk_reward_ratio,
            stop_loss_type=req.stop_loss_type,
            stop_loss_value=req.stop_loss_value,
            start_time=req.start_time,
            end_time=req.end_time,
            position_side=req.position_side,
            leverage=req.leverage,
            slippage_rate=req.slippage_rate,
        )

        if df.empty:
            return {"message": f"⚠️ {username}님의 전략 결과가 없습니다."}

        # ✅ DB 저장 (ON CONFLICT 중복 방지)
        with engine.begin() as conn:
            for _, row in df.iterrows():
                query = text(
                    """
                    INSERT INTO users.backtest_results (
                        google_id, symbol, interval, strategy_sql, risk_reward_ratio,
                        stop_loss_type, stop_loss_value,
                        start_time, end_time, entry_time, exit_time, result,
                        profit_rate, cum_profit_rate, created_at, updated_at,
                        position_side, leverage
                    )
                    VALUES (
                        :google_id, :symbol, :interval, :strategy_sql, :risk_reward_ratio,
                        :stop_loss_type, :stop_loss_value,
                        :start_time, :end_time, :entry_time, :exit_time, :result,
                        :profit_rate, :cum_profit_rate, NOW(), NOW(),
                        :position_side, :leverage
                    )
                    ON CONFLICT (google_id, symbol, interval, start_time, entry_time)
                    DO UPDATE SET
                        exit_time = EXCLUDED.exit_time,
                        result = EXCLUDED.result,
                        profit_rate = EXCLUDED.profit_rate,
                        cum_profit_rate = EXCLUDED.cum_profit_rate,
                        stop_loss_type = EXCLUDED.stop_loss_type,
                        stop_loss_value = EXCLUDED.stop_loss_value,
                        updated_at = NOW(),
                        position_side = EXCLUDED.position_side,
                        leverage = EXCLUDED.leverage;
                    """
                )
                conn.execute(
                    query,
                    {
                        "google_id": google_id,
                        "symbol": req.symbol,
                        "interval": req.interval,
                        "strategy_sql": req.strategy_sql,
                        "risk_reward_ratio": req.risk_reward_ratio,
                        "stop_loss_type": req.stop_loss_type,
                        "stop_loss_value": req.stop_loss_value,
                        "start_time": req.start_time,
                        "end_time": req.end_time,
                        "position_side": req.position_side,
                        "leverage": req.leverage,
                        **row.to_dict(),
                    },
                )

        return {
            "message": f"{username}님의 전략이 성공적으로 저장되었습니다.",
            "rows": len(df),
            "final_cum_profit_rate": float(df["cum_profit_rate"].iloc[-1]),
        }

    except Exception as e:
        print("❌ Error in save_strategy:", repr(e))
        raise HTTPException(status_code=500, detail=f"전략 실행 중 오류 발생: {e}")


# =========================
# 2️⃣ 필터링된 결과 조회
# =========================
@app.get("/filtered")
def get_filtered():
    try:
        data = get_filtered_data()
        return jsonable_encoder(data)
    except Exception as e:
        print("❌ Error in get_filtered:", repr(e))
        raise HTTPException(status_code=500, detail="DB 조회 실패")


# =========================
# 3️⃣ OHLCV 데이터 조회
# =========================
@app.get("/ohlcv/{symbol}/{interval}")
def get_ohlcv(symbol: str, interval: str):
    try:
        data = get_ohlcv_data(symbol, interval)
        return jsonable_encoder(data)
    except Exception as e:
        print("❌ Error in get_ohlcv:", repr(e))
        raise HTTPException(status_code=500, detail="OHLCV 조회 실패")


# =========================
# 4️⃣ Profit Rate 조회
# =========================
@app.get("/filtered-profit-rate")
def get_profit_rate():
    try:
        data = get_data_from_table(
            schema="trading_data",
            table_name="filtered",
            return_type=["entry_time", "profit_rate", "cum_profit_rate"],
        )
        return jsonable_encoder(data)
    except Exception as e:
        print("❌ Error in get_profit_rate:", repr(e))
        raise HTTPException(status_code=500, detail="Profit Rate 조회 실패")


# =========================
# 5️⃣ TP/SL 통계 조회
# =========================
@app.get("/filtered-tp-sl-rate")
def get_tp_sl_rate():
    try:
        return calculate_statics()
    except Exception as e:
        print("❌ Error in get_tp_sl_rate:", repr(e))
        raise HTTPException(status_code=500, detail="통계 계산 실패")


# =========================
# 6️⃣ Symbol 목록 조회
# =========================
@app.get("/symbols")
def get_symbols():
    try:
        with db_manager.get_sync_session() as session:
            query = text("SELECT symbol FROM metadata.crypto_info ORDER BY symbol ASC;")
            result = session.execute(query).fetchall()
            symbols = [r[0] for r in result]
            
            # ⚠️ DB가 비어있거나 연결 실패 시 Fallback
            if not symbols:
                print("⚠️ metadata.crypto_info is empty. Checking public.crypto_info...")
                try:
                    query_public = text("SELECT symbol FROM public.crypto_info ORDER BY symbol ASC;")
                    result_public = session.execute(query_public).fetchall()
                    symbols = [r[0] for r in result_public]
                except Exception as e:
                    print(f"⚠️ public.crypto_info query failed: {e}")

            if not symbols:
                print("⚠️ DB symbol list is empty. Using fallback.")
                return {"symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT"]}
                
            return {"symbols": symbols}
    except Exception as e:
        print("❌ Error in get_symbols:", repr(e))
        raise HTTPException(status_code=500, detail="심볼 목록 조회 실패")


# =========================
# 7️⃣ Interval 목록 조회
# =========================
@app.get("/debug_db")
def debug_db():
    results = {}
    try:
        with db_manager.get_sync_session() as session:
            # Check metadata schema
            try:
                cnt = session.execute(text("SELECT count(*) FROM metadata.crypto_info")).scalar()
                rows = session.execute(text("SELECT symbol FROM metadata.crypto_info LIMIT 5")).fetchall()
                results["metadata_count"] = cnt
                results["metadata_sample"] = [r[0] for r in rows]
            except Exception as e:
                results["metadata_error"] = str(e)

            # Check public schema
            try:
                cnt = session.execute(text("SELECT count(*) FROM public.crypto_info")).scalar()
                rows = session.execute(text("SELECT symbol FROM public.crypto_info LIMIT 5")).fetchall()
                results["public_count"] = cnt
                results["public_sample"] = [r[0] for r in rows]
            except Exception as e:
                results["public_error"] = str(e)
                
            # Check connection info
            results["db_url"] = str(session.bind.url).replace(":", "***")
            
    except Exception as e:
        results["connection_error"] = str(e)
        
    return results

@app.get("/intervals")
def get_intervals():
    try:
        # ⚙️ 스키마에 정의된 모든 interval 반환
        return ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "1d", "1w", "1M"]
    except Exception as e:
        print("❌ Error in get_intervals:", repr(e))
        raise HTTPException(status_code=500, detail="Interval 조회 실패")


# =========================
# 10️⃣ Stop Loss Options 조회
# =========================
@app.get("/sl-options")
def get_sl_options():
    return {
        "long": [
            {"value": "low", "label": "전봉 저가 (Low)"},
            {"value": "long_min_low_5", "label": "최근 5봉 최저가"},
            {"value": "long_min_low_20", "label": "최근 20봉 최저가"},
            {"value": "long_atr_2", "label": "ATR 2배 (Long)"},
        ],
        "short": [
            {"value": "high", "label": "전봉 고가 (High)"},
            {"value": "short_max_high_5", "label": "최근 5봉 최고가"},
            {"value": "short_max_high_20", "label": "최근 20봉 최고가"},
            {"value": "short_atr_2", "label": "ATR 2배 (Short)"},
        ],
    }


# =========================
# 8️⃣ 루트 경로
# =========================
@app.get("/")
def root():
    return {
        "message": "🚀 Backtesting API is running (JWT + StopLoss Custom + Conflict Safe)"
    }


# =========================
# 9️⃣ 심볼별 시간 범위 조회
# =========================
@app.get("/time-range/{symbol}/{interval}")
def get_time_range(symbol: str, interval: str):
    """
    trading_data.indicators_{interval} 테이블에서
    해당 symbol의 timestamp 최소/최대 범위를 조회합니다.
    
    ⚠️ OHLCV가 아닌 indicators 테이블 기준으로 조회하여,
    보조지표가 계산된 데이터만 백테스팅에 사용되도록 보장합니다.
    """
    try:
        # PostgreSQL은 따옴표로 감싸지 않으면 소문자로 변환하므로,
        # 테이블명을 따옴표로 감싸서 대소문자를 정확히 매칭
        table_name = f"indicators_{interval}"
        
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    f"""
                    SELECT 
                        MIN("timestamp") AS min_time,
                        MAX("timestamp") AS max_time
                    FROM trading_data."{table_name}"
                    WHERE symbol = :symbol
                    """
                ),
                {"symbol": symbol},
            ).fetchone()

        if not result or not result.min_time or not result.max_time:
            raise HTTPException(status_code=404, detail=f"{interval} 보조지표 데이터가 없습니다.")

        return {
            "symbol": symbol,
            "interval": interval,
            "min_time": str(result.min_time),
            "max_time": str(result.max_time),
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error in get_time_range ({symbol}, {interval}):", repr(e))
        raise HTTPException(status_code=500, detail=f"시간 범위 조회 실패: {e}")
