import requests

BASE_URL = "https://fapi.binance.com"  # USD-M 선물 REST 엔드포인트

def get_klines(symbol: str, interval: str = "1m", limit: int = 10):
    """
    바이낸스 USD-M 선물에서 캔들(OHLCV) 가져오기
    symbol: 예) "BTCUSDT"
    interval: 예) "1m", "5m", "1h", "1d"
    limit: 몇 개 가져올지 (최대 1500)
    """
    url = BASE_URL + "/fapi/v1/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
    }

    r = requests.get(url, params=params)
    r.raise_for_status()  # 에러면 예외 발생

    raw = r.json()  # 리스트 안에 리스트 구조

    candles = []
    for c in raw:
        candles.append({
            "open_time": c[0],
            "open": float(c[1]),
            "high": float(c[2]),
            "low": float(c[3]),
            "close": float(c[4]),
            "volume": float(c[5]),
            "close_time": c[6],
        })
    return candles

if __name__ == "__main__":
    data = get_klines("BTCUSDT", "1m", 5)
    print("캔들 개수:", len(data))
    for i, c in enumerate(data):
        print(f"[{i}] open={c['open']}, high={c['high']}, low={c['low']}, close={c['close']}, volume={c['volume']}")
