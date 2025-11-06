from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder
from get_data import get_ohlcv_data, get_filtered_data, get_data_from_table
from filtered_func import (
    run_conditional_lateral_backtest,
    save_result_to_table,
    calculate_statics,
)
from pydantic import BaseModel
from datetime import datetime as dt
from db_module.connect_sqlalchemy_engine import DBConnectionManager
from sqlalchemy import text
from typing import Optional

# DB 엔진
engine = DBConnectionManager.get_sync_engine()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class StrategyRequest(BaseModel):
    symbol: str
    interval: str
    strategy_sql: str
    risk_reward_ratio: float
    start_time: Optional[str] = None
    end_time: Optional[str] = None


@app.post("/save_strategy")
def save_strategy(req: StrategyRequest):
    try:
        df = run_conditional_lateral_backtest(
            symbol=req.symbol,
            interval=req.interval,
            strategy_sql=req.strategy_sql,
            risk_reward_ratio=req.risk_reward_ratio,
            start_time=req.start_time,
            end_time=req.end_time,
        )
        save_result_to_table(df)
        if df.empty:
            return {"message": "전략 실행, 결과 없음"}
        return {
            "message": "전략 실행 및 결과 저장 완료",
            "rows": len(df),
            "total_profit_rate": df["cum_profit_rate"].iloc[-1],
        }
    except Exception as e:
        print(repr(e))
        raise HTTPException(status_code=500, detail="전략 실행 중 오류 발생")


@app.get("/filtered")
def get_filtered():
    try:
        data = get_filtered_data()
        return jsonable_encoder(data)
    except Exception as e:
        print(repr(e))
        raise HTTPException(status_code=500, detail="DB 조회 실패")


@app.get("/ohlcv/{symbol}/{interval}")
def get_ohlcv(symbol: str, interval: str):
    try:
        data = get_ohlcv_data(symbol, interval)
        return jsonable_encoder(data)
    except Exception as e:
        print(repr(e))
        raise HTTPException(status_code=500, detail="OHLCV 조회 실패")


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
        raise HTTPException(status_code=500, detail="Profit Rate 조회 실패")


@app.get("/filtered-tp-sl-rate")
def get_tp_sl_rate():
    try:
        return calculate_statics()
    except Exception as e:
        raise HTTPException(status_code=500, detail="통계 계산 실패")
