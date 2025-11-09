# backtesting_backend/auth_utils.py
import os
from jose import jwt, JWTError
from fastapi import Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel

# === 환경 변수 ===
JWT_SECRET = os.getenv("JWT_SECRET", "capd")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "10080"))

# === OAuth2 스키마 (Authorization 헤더 자동 파싱) ===
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


# === 토큰에 담긴 데이터 구조 ===
class TokenData(BaseModel):
    id: str | None = None  # google_id
    sub: str | None = None  # email
    name: str | None = None  # username
    exp: int | None = None  # 만료 시각


# === JWT 검증 함수 ===
def verify_token(token: str = Depends(oauth2_scheme)) -> TokenData:
    """
    Authorization 헤더의 Bearer 토큰을 검증하고
    내부 Payload에서 사용자 정보를 추출한다.
    """
    try:
        payload = jwt.decode(
            token,
            JWT_SECRET,
            algorithms=[JWT_ALGORITHM],
            options={
                "require_exp": True,  # 만료 필드 필수
                "verify_signature": True,  # ✅ 명시적으로 서명 검증 활성화
            },
        )
        return TokenData(
            id=payload.get("id"),
            sub=payload.get("sub"),
            name=payload.get("name"),
            exp=payload.get("exp"),
        )
    except JWTError as e:
        print(f"❌ Invalid JWT: {repr(e)}")
        raise HTTPException(status_code=401, detail="유효하지 않은 JWT 토큰입니다.")
