"""Binance WebSocket API HMAC-SHA256 aláírás."""
import hashlib
import hmac
import time
import urllib.parse
from decimal import Decimal


def sign_params(params: dict, secret_key: str) -> dict:
    """
    Binance WebSocket API aláírás:
    1. Paraméterek ABC sorrendbe (apiKey, signature kivételével)
    2. query_string formátum
    3. HMAC-SHA256 hex digest
    """
    filtered = {k: v for k, v in params.items() if k not in ("signature",)}
    sorted_params = dict(sorted(filtered.items()))

    # Decimal-okat stringgé alakítjuk (trailing zeros nélkül)
    str_params = {}
    for k, v in sorted_params.items():
        if isinstance(v, Decimal):
            str_params[k] = f"{v:f}"
        else:
            str_params[k] = str(v)

    query_string = urllib.parse.urlencode(str_params)
    signature = hmac.new(
        secret_key.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    return {**sorted_params, "signature": signature}


def make_timestamp() -> int:
    """Aktuális időbélyeg milliszekundumban."""
    return int(time.time() * 1000)
