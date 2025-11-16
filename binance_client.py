# binance_client.py
import requests

BASE_URL = "https://fapi.binance.com"  # USD-M 선물 REST 엔드포인트

def fetch_klines(symbol: str, interval: str = "1m", limit: int = 100):
    """
    바이낸스 USD-M 선물에서 캔들(OHLCV) 가져오기
    return: [{symbol, interval, open_time, open, high, low, close, volume, close_time}, ...]
    """
    url = BASE_URL + "/fapi/v1/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
    }

    r = requests.get(url, params=params)
    r.raise_for_status()

    raw = r.json()

    candles = []
    for c in raw:
        candles.append({
            "symbol": symbol,
            "interval": interval,
            "open_time": c[0],          # ms
            "open": float(c[1]),
            "high": float(c[2]),
            "low": float(c[3]),
            "close": float(c[4]),
            "volume": float(c[5]),
            "close_time": c[6],         # ms
        })
    return candles

if __name__ == "__main__":
    # 모듈 단독 테스트용
    data = fetch_klines("BTCUSDT", "1m", 5)
    print("캔들 개수:", len(data))
    for i, c in enumerate(data):
        print(f"[{i}] open={c['open']}, high={c['high']}, low={c['low']}, close={c['close']}, volume={c['volume']}")
