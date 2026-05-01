"""Binance WebSocket API HMAC-SHA256 aláírás + clock drift kezelés."""
import hashlib
import hmac
import json
import time
import urllib.parse
import urllib.request
from decimal import Decimal

_time_offset_ms: int = 0


def sync_time_with_binance(rest_url: str = "https://api.binance.com") -> int:
    """
    Binance server time lekérése HTTP-en (szinkron, startup-kor hívandó).
    Beállítja a globális clock offset-et.
    Visszaadja az offset értékét ms-ban.
    """
    global _time_offset_ms
    try:
        url = f"{rest_url}/api/v3/time"
        req = urllib.request.Request(url, headers={"User-Agent": "kebo-grid/1.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
        server_ms = data["serverTime"]
        local_ms = int(time.time() * 1000)
        _time_offset_ms = server_ms - local_ms
        return _time_offset_ms
    except Exception as e:
        # Ha nem sikerül, nem blokkoljuk az indítást
        return 0


def set_time_offset(offset_ms: int) -> None:
    global _time_offset_ms
    _time_offset_ms = offset_ms


def make_timestamp() -> int:
    """Aktuális időbélyeg milliszekundumban, clock drift korrekcióval."""
    return int(time.time() * 1000) + _time_offset_ms


def sign_params(params: dict, secret_key: str) -> dict:
    """
    Binance WebSocket API aláírás:
    1. Paraméterek ABC sorrendbe (apiKey, signature kivételével)
    2. query_string formátum
    3. HMAC-SHA256 hex digest
    """
    filtered = {k: v for k, v in params.items() if k not in ("signature",)}
    sorted_params = dict(sorted(filtered.items()))

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
