# Kebo Grid Bot

> ⚠️ **FIGYELMEZTETÉS: Ez kereskedési szoftver. Csak Binance Testnet-en tesztelj, mielőtt valódi tőkét használsz!**
>
> A fejlesztők nem vállalnak felelősséget pénzügyi veszteségekért.
> Mindig értsd meg a kódot, mielőtt éles környezetben futtatod.

---

## Architektúra

**Teljesen asyncio-alapú, non-blocking, event-driven rendszer.**

### Task-ok (párhuzamosan futnak)

```
trading_ws_writer  ──┐
trading_ws_reader  ──┤──> event_dispatcher ──> grid_engine ──> order_router ──> ws_send_queue
user_stream        ──┘                     └──> db_writer
reconciliation
watchdog
api_server (FastAPI)
```

### Kulcs elvek

- **Csak WebSocket kereskedéshez** – REST kizárólag a listenKey megújításához
- **Non-blocking order submit** – `submit_order()` azonnal visszatér, nem vár ACK-ra
- **User data stream az igazság** – az order státusza kizárólag `executionReport`-ból ismert
- **Idempotens DB írások** – dupla event nem dupláz fill-t
- **Decimal mindenhol** – soha nem `float` ár/mennyiség értékekre

### Queue-alapú kommunikáció

```
ws_send_queue    → trading_ws_writer_task
event_queue      → event_dispatcher_task  (user stream és WS reader eseményei)
db_queue         → db_writer_task
command_queue    → command_processor_task (API parancsok)
broadcast_queue  → broadcast_task         (frontend WebSocket)
```

---

## Telepítés

### 1. Előfeltételek

- Python 3.12+
- PostgreSQL 15+ (kapcsolati adatok a `.env` fájlban: `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DATABASE_PASSWORD`)
- Binance API kulcsok (Testnet: https://testnet.binance.vision/)

### 2. Környezeti változók

```bash
cp .env.example .env
# Töltsd ki a .env fájlt:
# BINANCE_API_KEY=...
# BINANCE_SECRET_KEY=...
# DATABASE_PASSWORD=...
```

### 3. Konfiguráció

```bash
cp config.example.yaml config.yaml
# Szerkeszd a config.yaml fájlt igényeid szerint
# Alapértelmezett: testnet, SOLUSDT, 50 USDT tőke
```

### 4. Adatbázis migráció

```bash
pip install -e .
export PYTHONPATH=src
export DATABASE_PASSWORD=...
alembic upgrade head
```

### 5. Futtatás (lokális)

```bash
pip install -e .
export BINANCE_API_KEY=...
export BINANCE_SECRET_KEY=...
export DATABASE_PASSWORD=...
export PYTHONPATH=src
python src/app/main.py config.yaml
```

### 6. Futtatás (Docker)

```bash
cp .env.example .env
# Szerkeszd a .env fájlt
docker-compose up -d
docker-compose logs -f bot
```

---

## API Végpontok

Az API alapértelmezés szerint `http://localhost:8080`-on érhető el.

| Metódus | Végpont | Leírás |
|---------|---------|--------|
| GET | `/health` | Egészségügyi ellenőrzés |
| GET | `/status` | Bot állapot |
| GET | `/ws/status` | WebSocket kapcsolat állapot |
| GET | `/bot/runs` | Bot futások listája |
| GET | `/bot/runs/{id}` | Egy bot futás részletei |
| GET | `/bot/runs/{id}/orders` | Order-ek |
| GET | `/bot/runs/{id}/fills` | Fill-ek |
| GET | `/bot/runs/{id}/balances` | Egyenlegek |
| GET | `/bot/runs/{id}/pnl` | P&L |
| POST | `/bot/pause` | Bot szüneteltetése |
| POST | `/bot/resume` | Bot folytatása |
| POST | `/bot/stop` | Bot leállítása |
| POST | `/bot/emergency-stop` | **Vészleállítás** (azonnal visszatér) |
| WS | `/api/events` | Live esemény stream |

---

## Tesztek

```bash
pip install -e ".[dev]"
export PYTHONPATH=src
pytest src/tests/ -v
```

### Teszt lefedettség

- `test_grid_math.py` – Grid matematika (geometriai/aritmetikai)
- `test_precision.py` – Decimal kerekítés
- `test_state_machine.py` – Order állapotátmenetek
- `test_execution_report_idempotency.py` – Dupla event kezelés
- `test_manual_cancel.py` – Külső beavatkozás detektálás
- `test_emergency_stop.py` – Vészleállítás szekvencia
- `test_non_blocking_order_submit.py` – Non-blocking bizonyítás

---

## Konfiguráció részletei

Lásd: `config.example.yaml`

**Főbb paraméterek:**
- `bot.total_capital_quote` – Teljes tőke USDT-ben
- `bot.order_quote_value` – Egy order értéke USDT-ben
- `bot.target_net_profit_per_cycle_quote` – Célzott nettó profit ciklusonként
- `bot.grid_type` – `geometric` vagy `arithmetic`
- `fees.fee_mode` – `maker_assumed` (alapértelmezett)
- `anchor.source` – `manual` (saját ár megadása)

---

## Vészleállítás

A vészleállítás automatikusan elindul ha:
- User data stream > `max_user_stream_staleness_sec` másodperce nem küld eseményt
- DB writer queue > 90%-on tele
- Manuális: `POST /bot/emergency-stop`

Lépések:
1. Grid motor leáll (új order nem generálódik)
2. `openOrders.cancelAll` WS-en elküldve
3. CANCELED event-ek megvárása
4. Ha timeout: retry + alert

---

## Figyelmeztetések

- **Soha ne commitold az API kulcsokat vagy jelszavakat a git-be!**
- A `.gitignore` tartalmazza a `.env` fájlt
- Mindig testnet-en tesztelj először!
- A bot LIMIT_MAKER order típust használ (post-only) – ez megakadályozza a taker díj fizetését, de az order visszautasítódhat ha nem lesz maker
