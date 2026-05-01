# Kebo Grid Bot

> ⚠️ **FIGYELMEZTETÉS: Ez kereskedési szoftver. Csak Binance Testnet-en tesztelj, mielőtt valódi tőkét használsz!**
>
> A fejlesztők nem vállalnak felelősséget pénzügyi veszteségekért.
> Mindig értsd meg a kódot, mielőtt éles környezetben futtatod.

---

## Hogyan működik a bot?

### 1. Indulás – adatgyűjtés és grid felépítés

Amikor a bot elindul, az alábbi szekvencia zajlik le:

1. **Alembic migrációk** – automatikusan létrehozza/frissíti az adatbázis táblákat. Nem kell kézzel futtatni semmit.
2. **exchangeInfo lekérés** (Binance WebSocket API) – betölti a symbol paraméterei: tick size, step size, min notional, stb.
3. **Account lekérés** (Binance WebSocket API) – betölti az aktuális egyenleget (free/locked per asset).
4. **Anchor price meghatározás** – a konfig szerint:
   - `best_bid_ask_mid` *(alapértelmezett)*: a market stream bookTicker-éből számított bid/ask közép → ez lesz a grid közepe
   - `last_trade`: utolsó kötési ár
   - `manual`: konfig fájlban megadott fix ár
5. **Grid számítás** – a tőke, order méret, díjak és profit cél alapján kiszámítja a szükséges grid lépést (%), majd generálja a vételi és eladási szinteket az anchor ár köré.
6. **Kezdeti order-ek küldése** – az összes szint order-jét egyszerre, non-blocking módon küldi el a Binance-nek (nem vár ACK-ra).

### 2. Grid felépítés példa (geometriai, SOLUSDT, anchor: 170 USDT)

```
Sell szintek (LIMIT_MAKER):
  +4:  177.82 USDT  │ sell 0.028 SOL
  +3:  175.12 USDT  │ sell 0.028 SOL
  +2:  172.44 USDT  │ sell 0.028 SOL
  +1:  169.77 USDT  │ sell 0.028 SOL
──────────────── ANCHOR: 170.00 ────────────────
  -1:  167.33 USDT  │ buy  0.029 SOL
  -2:  164.91 USDT  │ buy  0.030 SOL
  -3:  162.50 USDT  │ buy  0.030 SOL
  -4:  160.12 USDT  │ buy  0.031 SOL
Buy szintek (LIMIT_MAKER):
```

A grid lépés (`r`) a következő feltételt teljesíti:
```
nettó profit / ciklus = V × r × (1 - fs) − V × (1 + fb) ≥ π
```
ahol `V` = order értéke USDT-ben, `fb`/`fs` = vétel/eladás díjak, `π` = célzott profit.

### 3. Order életciklus

```
PLANNED → SUBMIT_QUEUED → [WS küldés] → SUBMITTED_UNKNOWN
                                              │
                              user data stream executionReport
                                              │
                    ┌─────────────────────────┼────────────────┐
                    │                         │                │
                 WORKING            PARTIALLY_FILLED       REJECTED
                    │                         │
                 FILLED ◄────────────── FILLED
                    │
              counter order küldés (ellentétes irány)
```

**Fontos:** Az order státuszát kizárólag a Binance User Data Stream `executionReport` eseménye határozza meg. A WebSocket API ACK csak telemetria – nem authoritative.

### 4. Fill esemény → counter order

- **BUY FILLED** → SELL counter order a következő magasabb szinten (a ténylegesen vett mennyiséggel, jutalék levonva)
- **SELL FILLED** → BUY counter order a következő alacsonyabb szinten

Ez a ciklus addig ismétlődik, amíg a bot fut.

### 5. Külső beavatkozás kezelése

Ha valaki kézzel töröl egy order-t a Binance felületén:
- A bot `EXTERNAL_CANCELED`-ként jelöli az ordert
- `external_intervention_policy: pause` → bot szünetel, alert
- `external_intervention_policy: continue_reconcile` → fut tovább, rekonciliál
- `external_intervention_policy: emergency_stop` → vészleállítás

### 6. Vészleállítás

Automatikusan indul, ha:
- A user data stream > `max_user_stream_staleness_sec` másodperce nem küld eseményt
- A DB writer queue > 90%-os töltöttség
- Manuálisan: `POST /bot/emergency-stop`

Lépések: motor leáll → `openOrders.cancelAll` WS-en → CANCELED event-ek megvárása → retry ha maradnak → `EMERGENCY_STOPPED`

---

## Architektúra

**Teljesen asyncio-alapú, non-blocking, event-driven rendszer.**

### Task-ok (párhuzamosan futnak)

```
trading_ws_writer  ──┐
trading_ws_reader  ──┤──> event_dispatcher ──> grid_engine ──> order_router ──> ws_send_queue
user_stream        ──┘                     └──> db_writer
market_stream (anchor price + monitoring)
reconciliation
watchdog
api_server (FastAPI)
```

### Queue-alapú kommunikáció

```
ws_send_queue    → trading_ws_writer_task
event_queue      → event_dispatcher_task  (user stream eseményei)
db_queue         → db_writer_task
command_queue    → command_processor_task (API parancsok)
broadcast_queue  → broadcast_task         (frontend WebSocket)
```

### Kulcs elvek

- **Csak WebSocket kereskedéshez** – REST kizárólag a listenKey megújításához
- **Non-blocking order submit** – `submit_order()` azonnal visszatér, nem vár ACK-ra
- **User data stream az igazság** – az order státusza kizárólag `executionReport`-ból ismert
- **Idempotens DB írások** – dupla event nem dupláz fill-t
- **Decimal mindenhol** – soha nem `float` ár/mennyiség értékekre

---

## Adatbázis – táblák létrehozása

**A táblákat nem kell kézzel létrehozni.**

Minden indításkor a bot automatikusan futtatja az Alembic migrációkat:
```
bot indul → alembic upgrade head → táblák létrejönnek/frissülnek → bot fut
```

Ha a táblák már léteznek, az Alembic nem érinti őket (idempotens).

**Manuális migráció** (ha szükséges, pl. fejlesztés közben):
```bash
export DATABASE_PASSWORD=...
export DB_HOST=...
export PYTHONPATH=src
alembic upgrade head
```

**11 tábla:**
| Tábla | Tartalom |
|-------|----------|
| `bot_runs` | Bot futások, konfig snapshot |
| `grid_levels` | Grid szintek árai |
| `order_intents` | Beküldött order szándékok |
| `orders` | Exchange order-ek állapota |
| `fills` | Kötések (idempotens, UNIQUE trade_id) |
| `execution_events` | Összes executionReport esemény |
| `balances` | Egyenleg változások |
| `external_events` | Kézi beavatkozások |
| `ws_connections` | WebSocket kapcsolat log |
| `system_events` | Rendszer események, alertek |
| `api_audit` | API hívás napló |

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
# DB_HOST=...
```

### 3. Konfiguráció

```bash
# Egy pár esetén:
cp config.example.yaml config.yaml

# Több pár esetén (multi-instance):
cp config.example.yaml configs/solusdt.yaml
cp config.example.yaml configs/btcusdt.yaml
# Szerkeszd a YAML fájlokat (symbol, tőke, stb.)
```

### 4. Futtatás (lokális)

```bash
pip install -e .
source .env  # vagy: export BINANCE_API_KEY=... stb.
export PYTHONPATH=src
python src/app/main.py config.yaml
```

A bot automatikusan futtatja az Alembic migrációkat induláskor.

### 5. Futtatás (Docker – egy pár)

```bash
cp .env.example .env   # töltsd ki
cp config.example.yaml configs/solusdt.yaml  # szerkeszd
docker-compose up -d bot-solusdt
docker-compose logs -f bot-solusdt
```

### 6. Több pár párhuzamosan

Szerkeszd a `docker-compose.yml`-t, kommenteld ki a kívánt service-eket:

```bash
docker-compose up -d bot-solusdt bot-btcusdt
```

Minden botnak saját API portja van:
- SOLUSDT: http://localhost:8081
- BTCUSDT: http://localhost:8082

---

## API Végpontok

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

## Anchor price beállítás

| Érték | Leírás |
|-------|--------|
| `best_bid_ask_mid` | *(alapértelmezett)* Induláskor az aktuális legjobb bid/ask közepe |
| `last_trade` | Induláskor az utolsó kötési ár |
| `manual` | Fix ár a konfig fájlból (`anchor.manual_price`) |

---

## Tesztek

```bash
pip install -e ".[dev]"
export PYTHONPATH=src
pytest src/tests/ -v
```

---

## Figyelmeztetések

- **Soha ne commitold az API kulcsokat vagy jelszavakat a git-be!**
- A `.gitignore` kizárja: `.env`, `configs/*.yaml`, `.claude/`, `.idea/`
- Mindig testnet-en tesztelj először!
- A bot `LIMIT_MAKER` order típust használ (post-only) – ez megakadályozza a taker díj fizetését, de az order visszautasítódhat ha azonnal teljesülne (EXPIRED lesz, nem FILLED)
