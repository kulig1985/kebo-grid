# Kebo Grid Bot

> ⚠️ **FIGYELMEZTETÉS: Ez kereskedési szoftver. Csak Binance Testnet-en tesztelj, mielőtt valódi tőkét használsz!**
>
> A fejlesztők nem vállalnak felelősséget pénzügyi veszteségekért.

---

## Tartalomjegyzék

1. [Mi ez a bot és hogyan működik?](#1-mi-ez-a-bot-és-hogyan-működik)
2. [Kódfolyamat – mi történik indításkor?](#2-kódfolyamat--mi-történik-indításkor)
3. [Konfiguráció – minden mező magyarázata](#3-konfiguráció--minden-mező-magyarázata)
4. [Adatbázis – táblák és migrációk](#4-adatbázis--táblák-és-migrációk)
5. [Telepítés és futtatás](#5-telepítés-és-futtatás)
6. [Több pár párhuzamosan](#6-több-pár-párhuzamosan)
7. [API végpontok](#7-api-végpontok)
8. [Tesztek](#8-tesztek)

---

## 1. Mi ez a bot és hogyan működik?

### Grid trading alapelve

A grid bot egy meghatározott ársáv körül egymás fölé helyezett vételi és eladási limitárakat tart fenn. Amikor az ár mozog:

- Az ár **leesik** egy vételi szintre → **vásárol**
- A vásárlás teljesül → rögtön **elad** egy magasabb szinten
- Az eladás teljesül → rögtön **visszavásárol** az alacsonyabb szinten
- Ez a ciklus addig ismétlődik, amíg a bot fut

A profit az egyes buy-sell ciklusok különbözetéből keletkezik. A bot ezt a különbözetet matematikailag garantáltan profitálisnál nagyobbra állítja be – figyelembe véve a trading díjakat.

### Grid felépítés – konkrét példa

SOLUSDT, 50 USDT tőke, 5 USDT/order, anchor: 170 USDT (induláskor aktuális ár):

```
                    SELL LIMIT_MAKER order-ek
  +4. szint │  178.50 USDT │ 0.028 SOL
  +3. szint │  175.80 USDT │ 0.028 SOL
  +2. szint │  173.10 USDT │ 0.028 SOL
  +1. szint │  170.45 USDT │ 0.029 SOL
  ──────────┼──────────────┼──────────── ANCHOR: 170.00 USDT (induláskor aktuális ár)
  -1. szint │  167.33 USDT │ 0.029 SOL
  -2. szint │  164.75 USDT │ 0.030 SOL
  -3. szint │  162.19 USDT │ 0.030 SOL
  -4. szint │  159.65 USDT │ 0.031 SOL
                    BUY LIMIT_MAKER order-ek
```

**Egy ciklus profit számítása** (geometriai grid, r = 1.0163 ≈ 1.63% lépés):
```
Vásárlás:  5.00 USDT × (1 + 0.001) = 5.005 USDT kiadás
Eladás:    5.00 USDT × 1.0163 × (1 - 0.001) = 5.074 USDT bevétel
Nettó profit: 5.074 - 5.005 = 0.069 USDT / ciklus
```

### Order életciklus

```
PLANNED
  │ router.submit_order() hívja – azonnal visszatér
  ▼
SUBMIT_QUEUED
  │ ws_send_queue-ba kerül
  ▼
[Binance WS API: order.place küldés – NEM VÁR VÁLASZT]
  ▼
SUBMITTED_UNKNOWN
  │ Várjuk a user data stream visszajelzését
  ▼
executionReport érkezik (Binance User Data Stream)
  │
  ├─ x=NEW    → WORKING       (az order benn van a könyvben)
  ├─ x=TRADE  → PARTIALLY_FILLED vagy FILLED
  ├─ x=CANCELED + helyi kérelem → CANCELED
  ├─ x=CANCELED + NEM volt helyi kérelem → EXTERNAL_CANCELED ⚠️
  └─ x=REJECTED → REJECTED    (pl. PRICE_FILTER sértés)

FILLED → counter order küldés a következő szinten
```

**Kulcsszabály:** A bot soha nem REST API-n kérdezi le az order állapotát. Kizárólag a Binance User Data Stream `executionReport` eseménye az igazság forrása.

---

## 2. Kódfolyamat – mi történik indításkor?

### `python src/app/main.py config.yaml`

```
app/main.py → main()
│
├─ 1. load_config("config.yaml")
│     └─ app/config.py: YAML beolvasás + Pydantic validáció
│        Ellenőrzi: anchor.manual_price kötelező ha source=manual, stb.
│
├─ 2. setup_logging()
│     └─ app/logging.py: structlog JSON formátum beállítás
│
├─ 3. init_db()
│     └─ persistence/db.py: SQLAlchemy async engine létrehozás
│        DSN: postgresql+asyncpg://user:pass@host:port/db
│
├─ 4. run_migrations()           ← TÁBLÁK ITT JÖNNEK LÉTRE
│     └─ alembic upgrade head
│        persistence/migrations/001_initial_schema.py fut le
│        11 tábla létrehozás (ha még nem létezik)
│
├─ 5. Queue-k létrehozása (asyncio.Queue)
│     ws_send_queue  – WS küldendő parancsok
│     event_queue    – user stream események
│     db_queue       – DB írási feladatok
│     command_queue  – API parancsok (pause/stop/stb.)
│     broadcast_queue – frontend live events
│
├─ 6. Komponensek példányosítása
│     BinanceWsApi(config, ws_send_queue, db_queue)
│     UserDataStream(config, event_queue)
│     MarketStream(config, "SOLUSDT")
│     GridEngine(settings, ws_api, db_queue, event_queue, market_stream)
│     DbWriter(db_queue)
│     EmergencyStop(engine, ws_api, db_queue, safety_config)
│     Reconciliation(engine, ws_api, db_queue, safety_config)
│     Watchdog(engine, ws_api, user_stream, db_writer, emergency, safety_config)
│
├─ 7. BotRun rekord létrehozása DB-ben
│     bot_runs táblába kerül: symbol, konfig snapshot, INITIALIZING státusz
│
├─ 8. FastAPI app indítás (uvicorn, port 8080)
│
└─ 9. asyncio.TaskGroup – MIND PÁRHUZAMOSAN FUT:
      ┌─ ws_api.writer_loop()     exchange/ws_api.py
      ├─ ws_api.reader_loop()     exchange/ws_api.py
      ├─ user_stream.start()      exchange/user_stream.py
      ├─ market_stream.start()    exchange/market_stream.py
      ├─ db_writer.run()          persistence/writer.py
      ├─ event_dispatcher()       app/main.py
      ├─ command_processor()      app/main.py
      ├─ reconciliation.run()     supervisor/reconciliation.py
      ├─ watchdog.run()           supervisor/watchdog.py
      ├─ broadcast_loop()         api/live_events.py
      ├─ uvicorn.serve()          (FastAPI szerver)
      └─ [2 mp várakozás] → engine.initialize()   grid/engine.py
```

### `engine.initialize()` – a grid felépítés

```
grid/engine.py → initialize()
│
├─ 1. ws_api.get_exchange_info("SOLUSDT")
│     Lekéri: tick_size, step_size, min_qty, min_notional, max_orders
│     exchange/ws_api.py: WS API "exchangeInfo" metódus hívás
│     Visszatérés: SymbolInfo objektum (exchange/models.py)
│
├─ 2. ws_api.get_account()
│     Lekéri: szabad és zárolt egyenleg minden asset-re
│     grid/inventory.py → InventoryManager.update_from_account()
│
├─ 3. _determine_anchor_price()
│     Ha anchor.source = "best_bid_ask_mid":
│       exchange/market_stream.py → wait_for_price(timeout=15s)
│       Megvárja az első bookTicker üzenetet a Binance-től
│       Visszatér: (best_bid + best_ask) / 2 → ez lesz a GRID KÖZEPE
│
├─ 4. Grid lépés számítás (grid/calculator.py)
│     Geometriai esetén:
│       r = (1 + fee_buy + target_profit/order_value) / (1 - fee_sell)
│       grid_step_pct = r - 1
│       Ellenőrzés: min_grid_step_pct ≤ grid_step_pct ≤ max_grid_step_pct
│
├─ 5. K_buy, K_sell számítás
│     available = total_capital × (1 - quote_reserve_pct)
│     K_total = floor(available / order_quote_value)
│     K_buy = floor(K_total × buy_allocation_ratio)
│     K_sell = K_total - K_buy
│
├─ 6. Grid szintek generálása
│     Buy szintek:  anchor / r^1, anchor / r^2, ..., anchor / r^K_buy
│     Sell szintek: anchor × r^1, anchor × r^2, ..., anchor × r^K_sell
│     Minden szint: ár kerekítés tick_size-ra, mennyiség kerekítés step_size-ra
│     Validáció: min_qty, min_notional ellenőrzés minden szintnél
│
└─ 7. _place_initial_orders() – ÖSSZES ORDER NON-BLOCKING
      grid/order_router.py → submit_order() minden szintre:
        ① client_order_id generálás (pl. "G-000001-B-001-0000-01")
        ② db_queue.put_nowait(DbEvent "upsert_order_from_intent") → SUBMIT_QUEUED
        ③ ws_send_queue.put_nowait(WsSendCommand "order.place")
        ④ RETURN AZONNAL – nem vár semmire
```

### Mit csinálnak a háttér task-ok?

**`ws_api.writer_loop()`** `(exchange/ws_api.py)`
```
ws_send_queue-ból olvas
  → HMAC-SHA256 aláírás hozzáadása (exchange/signing.py)
  → Binance WS API-ra küldés (wss://ws-api.binance.com)
```

**`ws_api.reader_loop()`** `(exchange/ws_api.py)`
```
Binance WS API válaszait olvassa
  → Ha hiba: db_queue-ba log_system_event
  → ACK: csak telemetria, NEM authoritative
```

**`user_stream.start()`** `(exchange/user_stream.py)`
```
1. REST POST /api/v3/userDataStream → listenKey megszerzése
2. WS csatlakozás: wss://stream.binance.com/ws/<listenKey>
3. Minden esemény → event_queue.put_nowait() – SEMMI MÁS NEM TÖRTÉNIK ITT
4. 30 percenként: REST PUT → listenKey megújítás
```

**`event_dispatcher()`** `(app/main.py)`
```
event_queue-ból olvas:
  executionReport:
    → state_machine.transition() – új order státusz meghatározása
    → db_queue: insert_execution_event (idempotens)
    → db_queue: update_order_from_execution
    → Ha TRADE esemény: db_queue: insert_fill (idempotens, UNIQUE trade_id)
    → Ha FILLED: engine.on_execution_report() → counter order küldés
    → Ha EXTERNAL_CANCELED: engine.on_external_cancel() → policy alapján
  outboundAccountPosition:
    → inventory.update_from_account_position()
    → db_queue: update_balance
  balanceUpdate:
    → inventory.update_from_balance_update()
```

**`db_writer.run()`** `(persistence/writer.py)`
```
db_queue-ból olvas → PostgreSQL async write
Idempotens: ON CONFLICT DO NOTHING minden kritikus helyen
Ha DB nem elérhető: DEGRADED állapot, queue-ban várakozik
```

**`watchdog.run()`** `(supervisor/watchdog.py)`
```
5 másodpercenként ellenőriz:
  - user_stream.last_event_age_sec > max_user_stream_staleness_sec? → emergency_stop
  - db_writer.queue_size > 90% of max? → emergency_stop
  - db_writer.is_degraded? → WARNING log
```

### Fill után mi történik?

```
Binance: BUY order teljesül (167.33 USDT)
  ↓
user_stream → event_queue: executionReport (x=TRADE, X=FILLED)
  ↓
event_dispatcher → engine.on_execution_report()
  ↓
engine._on_order_filled()
  level_index = -1 (ez volt a -1. buy szint)
  next_sell_index = 0  (a következő magasabb = anchor szint)
  qty = filled_qty - base_commission (ha jutalék SOL-ban volt fizetve)
  ↓
router.submit_order(SELL, price=170.45, qty=0.029)
  ↓
db_queue + ws_send_queue → AZONNAL visszatér
  ↓
Binance: SELL order az 170.45-ös szinten benn van a könyvben
```

---

## 3. Konfiguráció – minden mező magyarázata

### `exchange` – tőzsde kapcsolat

```yaml
exchange:
  env: testnet              # "testnet" vagy "mainnet"
                            # testnet: nem valódi pénz, biztonságos teszteléshez
  
  ws_api_url_mainnet: wss://ws-api.binance.com:443/ws-api/v3
  ws_api_url_testnet: wss://ws-api.testnet.binance.vision/ws-api/v3
                            # WebSocket API URL-ek – ne módosítsd
  
  user_stream_url_mainnet: wss://stream.binance.com:9443/ws
  user_stream_url_testnet: wss://stream.testnet.binance.vision:9443/ws
                            # User Data Stream URL-ek – ne módosítsd
  
  rest_url_mainnet: https://api.binance.com
  rest_url_testnet: https://testnet.binance.vision
                            # REST API URL (csak listenKey megújításhoz kell)
  
  api_key_env: BINANCE_API_KEY
  secret_key_env: BINANCE_SECRET_KEY
                            # Az env var NEVE, ahonnan a bot a kulcsot olvassa
                            # Maga a kulcs a .env fájlban van, YAML-ban SOHA
  
  recv_window_ms: 5000      # WS API kérések érvényességi ablaka (ms)
                            # Ha a szerver késés > 5000ms, a kérés elutasítva
```

### `bot` – kereskedési paraméterek

```yaml
bot:
  symbol: SOLUSDT           # Binance symbol – pontosan így, ahogy a tőzsdén
  base_asset: SOL           # A vett/eladott eszköz (pl. SOL)
  quote_asset: USDT         # Az árfolyam eszköz (pl. USDT)
  
  total_capital_quote: "50"
  # Mennyi USDT-t kezel ez a bot összesen.
  # Ebből vonjuk le a tartalékot, ebből jönnek a buy order-ek és
  # az initial sell order-ekhez szükséges base eszköz fedezete.
  
  order_quote_value: "5"
  # Egy order értéke USDT-ben.
  # Pl. 5 USDT: minden vételi order ~5 USDT értékű SOL-t vesz,
  # minden eladási order ~5 USDT értékű SOL-t ad el.
  # Fontos: Binance min_notional filter-nek teljesülnie kell (általában 1-5 USDT).
  
  target_net_profit_per_cycle_quote: "0.02"
  # Célzott MINIMÁLIS nettó profit egy buy-sell cikluson (USDT-ben).
  # A bot ebből SZÁMOLJA KI a szükséges grid lépést:
  #   r = (1 + fee_buy + 0.02/5) / (1 - fee_sell)
  # Minél nagyobb ez az érték, annál szélesebb lesz a grid lépés.
  
  grid_type: geometric
  # "geometric": a szintek közötti távolság % arányos (r-szoros)
  #   → alacsonyabb áron kisebb az abszolút USD távolság, magasabban nagyobb
  #   → ez a természetesebb volatilitáshoz
  # "arithmetic": a szintek közötti távolság fix USDT összeg
  #   → minden szinten azonos USD távolság
  
  inventory_mode: prebalanced
  # "prebalanced": van meglévő base eszközöd (SOL), azt használja sell order-ekhez
  # "use_existing_balances": hasonló, meglévő egyenleget használja
  # "quote_only_bootstrap": csak USDT-d van, először vásárol SOL-t,
  #   majd utána rak fel sell order-eket
  
  buy_allocation_ratio: "0.5"
  # A tőke mekkora hányada megy buy szintekre vs sell szintekre.
  # 0.5 = 50-50 arány. Ha több buy oldalt szeretnél: "0.6" vagy "0.7".
  
  quote_reserve_pct: "0.02"
  # A USDT egyenleg hány %-át NE helyezze orderbe (tartalék).
  # 0.02 = 2% tartalék, hogy ne merüljön ki teljesen a USDT.
  
  base_reserve_pct: "0.02"
  # A base eszköz (SOL) egyenleg hány %-át NE helyezze orderbe.
  # 0.02 = 2% tartalék.
  
  order_type: LIMIT_MAKER
  # "LIMIT_MAKER": post-only order – csak akkor teljesül, ha nem azonnal
  #   (maker díjat fizet, ami általában alacsonyabb vagy nulla)
  #   Ha azonnal teljesülne, a Binance visszautasítja (EXPIRED státusz)
  # "LIMIT": hagyományos limit order – teljesülhet taker-ként is
  
  time_in_force: GTC        # Good Till Canceled – addig él, amíg kézzel nem törlöd
  
  max_grid_levels: 20
  # Maximum hány grid szint lehet összesen (buy + sell).
  # Ha a tőke több szintet engedne meg, ez a korlát felülírja.
  
  min_grid_step_pct: "0.0025"
  # ⚠️ EZ NEM A GRID LÉPÉS – ez egy ALSÓ BIZTONSÁGI KORLÁT.
  # A tényleges grid lépést a bot a fee + profit célból SZÁMÍTJA.
  # Ha a számított lépés kisebb lenne ennél: ez az érték érvényesül.
  # 0.0025 = 0.25% minimum – ennél szorosabb gridnek nincs értelme
  # (slippage, spread elviszi a profitot).
  
  max_grid_step_pct: "0.05"
  # ⚠️ EZ SEM A GRID LÉPÉS – ez egy FELSŐ BIZTONSÁGI KORLÁT.
  # Ha a számított lépés meghaladná ezt, a bot hibával leáll.
  # 0.05 = 5% maximum – ennél szélesebb grid esetén valószínűleg
  # elírás történt a profit célban vagy a díjakban.
```

### `fees` – trading díjak

```yaml
fees:
  fee_mode: maker_assumed
  # "maker_assumed": minden order LIMIT_MAKER → maker díj érvényes
  # "taker_assumed": ha LIMIT order-t használsz és taker is lehetsz
  # "custom": maker_fee_buy és taker_fee_sell eltérő értékeket adj meg
  
  maker_fee_buy: "0.001"    # 0.1% – Binance standard alap díj
  maker_fee_sell: "0.001"   # 0.1% – Binance standard alap díj
  taker_fee_buy: "0.001"    # taker mód esetén érvényes
  taker_fee_sell: "0.001"
```

**Miért kell manuálisan megadni a díjakat?**

A Binance az egyes felhasználók díjait nem adja vissza a publikus WebSocket API-n. A díj függ:
- A te VIP szintedtől (kereskedési volumen alapján)
- Hogy tartasz-e BNB-t (25% kedvezmény BNB-vel fizetéskor)
- Esetleges referral kedvezményektől

A standard Binance Spot díj: **0.1% maker = 0.1% taker** (0.001).
Ha BNB-vel fizeted a díjat: **0.075%** (0.00075).
Ha VIP1+ vagy: kevesebb.

A bot ezeket a díjakat arra használja, hogy **kiszámolja a minimálisan szükséges grid lépést**, ami felett a ciklus profitábilis. Ha rosszul adod meg (pl. 0-t), a grid túl szoros lesz és veszteséges lesz minden ciklus.

**Hol nézd meg a te díjaidat?** Binance → Felhasználói menü → Díjak.

### `anchor` – a grid közepe

```yaml
anchor:
  source: best_bid_ask_mid
  # "best_bid_ask_mid" (ajánlott): induláskor az aktuális bid/ask közép
  #   → a grid automatikusan az aktuális piac köré épül
  # "last_trade": az utolsó kötési ár
  # "manual": fix ár – be kell írni manual_price értékét
  
  # manual_price: "170"     # csak source: manual esetén töltsd ki
```

### `safety` – biztonsági korlátok

```yaml
safety:
  external_intervention_policy: pause
  # Mi történjen, ha kézzel törölsz egy order-t a Binance felületén:
  # "pause": bot szünetel, értesítés – ajánlott!
  # "continue_reconcile": fut tovább, megpróbálja javítani az állapotot
  # "emergency_stop": azonnal leáll és töröl mindent
  
  max_user_stream_staleness_sec: 10
  # Ha a user data stream 10 másodpercnél régebben nem küldött eseményt,
  # a watchdog vészleállítást indít (a bot "vakon" kereskedne).
  
  max_trading_ws_staleness_sec: 10
  # Hasonló a WS API kapcsolatra.
  
  cancel_retry_interval_sec: 2  # Vészleállításnál cancelAll újrapróbálási időköz
  cancel_retry_max: 5           # Maximum újrapróbálkozás száma
  
  emergency_stop_on_db_queue_full: true
  # Ha a DB írási sor megtelik (DB nem elérhető), vészleállítás.
  # Ha false: a bot fut tovább de elveszíti az adatokat.
  
  emergency_stop_on_balance_mismatch: true
  # Ha az egyenleg eltér a várttól (ismeretlen tranzakció), vészleállítás.
  
  sell_on_emergency_stop: false
  # Vészleállításkor eladja-e a meglévő base eszközt (SOL)?
  # false (ajánlott): csak az order-eket törli, a SOL marad nálad.
  # true: piaci áron eladja az összes SOL-t (stop-loss jellegű viselkedés).
```

### `database` – adatbázis kapcsolat

```yaml
database:
  host: localhost           # DB szerver IP vagy hostname (env: DB_HOST)
  port: 5432                # PostgreSQL port (env: DB_PORT)
  name: kebo_db             # Adatbázis neve (env: DB_NAME)
  user: kebo_grid           # Adatbázis felhasználó (env: DB_USER)
  password_env: DATABASE_PASSWORD
                            # Az env var neve, ahol a jelszó van (.env fájlban)
  pool_min_size: 1          # Minimum állandó DB kapcsolat
  pool_max_size: 10         # Maximum egyidejű DB kapcsolat
  writer_queue_max_size: 10000
                            # Maximum ennyi DB írási feladat várakozhat sorban
```

### `logging` – naplózás

```yaml
logging:
  level: INFO   # DEBUG | INFO | WARNING | ERROR
  json: true    # true: gépi JSON formátum (Docker/log aggregátor)
                # false: olvasható szöveges formátum (fejlesztés)
```

---

## 4. Adatbázis – táblák és migrációk

**A táblákat nem kell kézzel létrehozni.**

A bot indításkor **automatikusan** futtatja az Alembic migrációkat:

```
python src/app/main.py config.yaml
  ↓
run_migrations()        ← app/main.py
  ↓
alembic upgrade head    ← alembic/env.py + versions/001_initial_schema.py
  ↓
11 tábla létrehozás     ← csak ha még nem léteznek (idempotens)
  ↓
bot fut tovább
```

Ha a táblák már léteznek, az Alembic nem érinti őket.

**Manuális migráció** (fejlesztés, debug):
```bash
export DATABASE_PASSWORD=... DB_HOST=...
export PYTHONPATH=src
alembic upgrade head
```

| Tábla | Mire való |
|-------|-----------|
| `bot_runs` | Minden bot indítás rögzítve, konfig snapshot |
| `grid_levels` | A generált grid szintek árai |
| `order_intents` | Minden beküldési szándék (mielőtt Binance-re megy) |
| `orders` | Exchange oldalon lévő order-ek és állapotuk |
| `fills` | Kötések – UNIQUE(symbol, order_id, trade_id): dupla event nem dupláz |
| `execution_events` | Összes Binance executionReport esemény nyers formában |
| `balances` | Egyenleg változások időbélyeggel |
| `external_events` | Kézi beavatkozások (pl. kézzel törölt order) |
| `ws_connections` | WebSocket kapcsolat log (csatlakozás, disconnect, ping/pong) |
| `system_events` | Rendszer alertek, watchdog események |
| `api_audit` | API hívások naplója |

---

## 5. Telepítés és futtatás

### Előfeltételek

- Python 3.12+
- PostgreSQL 15+
- Binance API kulcsok → [Testnet](https://testnet.binance.vision/) | [Mainnet](https://www.binance.com/en/my/settings/api-management)

### Lépések

```bash
# 1. Klónozás
git clone https://github.com/kulig1985/kebo-grid.git
cd kebo-grid

# 2. Függőségek
pip install -e .

# 3. Env változók
cp .env.example .env
# Szerkeszd: BINANCE_API_KEY, BINANCE_SECRET_KEY, DATABASE_PASSWORD, DB_HOST, stb.

# 4. Konfig
cp configs/solusdt.example.yaml configs/solusdt.yaml
# Szerkeszd: total_capital_quote, order_quote_value, env (testnet/mainnet), stb.

# 5. Futtatás (táblák automatikusan létrejönnek)
export PYTHONPATH=src
source .env   # vagy: set -a; source .env; set +a
python src/app/main.py configs/solusdt.yaml
```

### Docker

```bash
cp .env.example .env                                   # töltsd ki
cp configs/solusdt.example.yaml configs/solusdt.yaml  # szerkeszd
docker-compose up -d bot-solusdt
docker-compose logs -f bot-solusdt
```

---

## 6. Több pár párhuzamosan

Minden pár egy külön Docker konténerben fut, saját konfiggal és saját API porttal.

```bash
# Sablonok másolása és szerkesztése
cp configs/solusdt.example.yaml configs/solusdt.yaml  # port: 8081
cp configs/btcusdt.example.yaml configs/btcusdt.yaml  # port: 8082

# docker-compose.yml-ben komment eltávolítás a bot-btcusdt service-nél
# Mindkettő indítása
docker-compose up -d bot-solusdt bot-btcusdt
```

Minden bot teljesen független: saját egyenleg, saját grid, saját API.

---

## 7. API végpontok

Alap URL: `http://localhost:8081` (SOLUSDT bot)

| Metódus | Végpont | Leírás |
|---------|---------|--------|
| GET | `/health` | Él-e a szerver? |
| GET | `/status` | Bot státusz, szint számok |
| GET | `/ws/status` | WebSocket kapcsolatok állapota |
| GET | `/bot/runs` | Összes bot futás listája |
| GET | `/bot/runs/{id}/orders` | Order-ek (szűrhető: `?status=WORKING`) |
| GET | `/bot/runs/{id}/fills` | Teljesült kötések |
| GET | `/bot/runs/{id}/balances` | Egyenleg napló |
| GET | `/bot/runs/{id}/pnl` | P&L összesítő |
| POST | `/bot/pause` | Grid megáll, nyitott order-ek maradnak |
| POST | `/bot/resume` | Grid folytatás |
| POST | `/bot/emergency-stop` | Azonnali leállítás + cancelAll |
| WS | `/api/events` | Valós idejű esemény stream |

---

## 8. Tesztek

```bash
pip install -e ".[dev]"
export PYTHONPATH=src
pytest src/tests/ -v
```

| Tesztfájl | Mit bizonyít |
|-----------|-------------|
| `test_grid_math.py` | A számított grid lépés fedezi a díjakat és a profit célt |
| `test_precision.py` | Decimal kerekítés soha nem okoz float pontatlanságot |
| `test_state_machine.py` | Minden order állapotátmenet helyes |
| `test_execution_report_idempotency.py` | Dupla event nem dupláz fill-t |
| `test_manual_cancel.py` | Kézi törlés detektálás + bot szünetel |
| `test_emergency_stop.py` | Vészleállítás < 500ms, cancelAll beküldve |
| `test_non_blocking_order_submit.py` | submit_order() nem vár ACK-ra |

---

## Figyelmeztetések

- **Soha ne commitold a `.env` fájlt vagy az éles `configs/*.yaml` fájlokat!**
- A `.gitignore` automatikusan kizárja ezeket.
- **Testnet-en tesztelj először** – a valódi pénz elveszhet.
- A `LIMIT_MAKER` order visszautasítódik (EXPIRED), ha azonnal teljesülne. Ez normális – a bot nem generál counter order-t EXPIRED esetén.
