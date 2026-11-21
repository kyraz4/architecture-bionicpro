import os
from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Annotated, Any
import jwt
import clickhouse_connect

app = FastAPI(title="BionicPRO Reports API")

origins = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS", "PUT", "DELETE"],
    allow_headers=["Authorization", "Content-Type"],
)

# --- ClickHouse ---
ch_client = clickhouse_connect.get_client(
    host=os.getenv("CLICKHOUSE_HOST", "localhost"),
    port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
    username=os.getenv("CLICKHOUSE_USER", "default"),
    password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    database=os.getenv("CLICKHOUSE_DB", "reports_mart"),
)

# --- JWT / Keycloak ---
KEYCLOAK_URL = os.getenv("KEYCLOAK_URL", "http://localhost:8080")
KEYCLOAK_REALM = os.getenv("KEYCLOAK_REALM", "reports-realm")
JWKS_URL = f"{KEYCLOAK_URL}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/certs"

jwks_client = jwt.PyJWKClient(JWKS_URL, cache_keys=True, lifespan=600, timeout=5)
bearer_scheme = HTTPBearer(auto_error=False)


def unauthorized(detail: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=detail,
        headers={"WWW-Authenticate": "Bearer"},
    )


def verify_jwt(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)],
) -> dict[str, Any]:
    """Проверяет JWT от Keycloak и возвращает claims."""
    if credentials is None:
        raise unauthorized("Missing bearer token")
    token = credentials.credentials
    try:
        signing_key = jwks_client.get_signing_key_from_jwt(token)
        claims = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            options={"require": ["exp", "iat", "sub"]},
        )
        return claims
    except jwt.ExpiredSignatureError:
        raise unauthorized("Token expired")
    except jwt.PyJWKClientError:
        raise unauthorized("Unknown signing key")
    except jwt.InvalidTokenError:
        raise unauthorized("Invalid token")


@app.get("/reports")
def get_reports(claims: Annotated[dict[str, Any], Depends(verify_jwt)]):
    """
    Возвращает отчёты ТОЛЬКО для текущего пользователя.
    user_id берётся из проверенного JWT (claim sub) — это UUID пользователя Keycloak.
    В витрине ClickHouse поле user_id должно содержать этот же UUID.
    """
    user_id = claims.get("sub")
    if not user_id:
        raise HTTPException(status_code=401, detail="No subject in token")

    query = """
        SELECT
            report_date,
            prosthesis_id,
            total_signals,
            recognized_moves,
            avg_response_ms,
            error_count
        FROM prosthesis_usage_daily
        WHERE user_id = %(user_id)s
        ORDER BY report_date DESC
        LIMIT 100
    """
    try:
        result = ch_client.query(query, parameters={"user_id": user_id})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    columns = ["report_date", "prosthesis_id", "total_signals",
               "recognized_moves", "avg_response_ms", "error_count"]
    return {
        "user_id": user_id,
        "reports": [dict(zip(columns, row)) for row in result.result_rows]
    }


@app.get("/health")
def health():
    return {"status": "ok"}
