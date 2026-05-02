# Kebo Grid Bot

> **FIGYELMEZTETÉS: Ez kereskedési szoftver. Csak Binance Testnet-en tesztelj, mielőtt valódi tőkét használsz!**
>
> A fejlesztők nem vállalnak felelősséget pénzügyi veszteségekért.

---

## Tartalomjegyzék

1. [Mi ez a bot és hogyan működik?](#1-mi-ez-a-bot-és-hogyan-működik)
2. [Kódfolyamat – mi történik indításkor?](#2-kódfolyamat--mi-történik-indításkor)
3. [Grid Map – O(1) counter order lookup](#3-grid-map--o1-counter-order-lookup)
4. [Counter Order logika – fill utáni azonnali reakció](#4-counter-order-logika--fill-utáni-azonnali-reakció)
5. [Recovery – újraindulás bent ragadt orderekből](#5-recovery--újraindulás-bent-ragadt-orderekből)
6. [DB Persistence – grid szintek mentése](#6-db-persistence--grid-szintek-mentése)
7. [Konfiguráció – minden mező magyarázata](#7-konfiguráció--minden-mező-magyarázata)
8. [Adatbázis – táblák és migrációk](#8-adatbázis--táblák-és-migrációk)
9. [Telepítés és futtatás](#9-telepítés-és-futtatás)
10. [Több pár párhuzamosan](#10-több-pár-párhuzamosan)
11. [API végpontok](#11-api-végpontok)
12. [Tesztek](#12-tesztek)

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
  +4. szint │  178.50 USDT │ 0.028 SOL    │ grid_map[+4]
  +3. szint │  175.80 USDT │ 0.028 SOL    │ grid_map[+3]
  +2. szint │  173.10 USDT │ 0.028 SOL    │ grid_map[+2]
  +1. szint │  170.45 USDT │ 0.029 SOL    │ grid_map[+1]
  ──────────┼──────────────┼──────────────┼──── ANCHOR: 170.00 USDT │ grid_map[0]
  -1. szint │  167.33 USDT │ 0.029 SOL    │ grid_map[-1]
  -2. szint │  164.75 USDT │ 0.030 SOL    │ grid_map[-2]
  -3. szint │  162.19 USDT │ 0.030 SOL    │ grid_map[-3]
  -4. szint │  159.65 USDT │ 0.031 SOL    │ grid_map[-4]
                    BUY LIMIT_MAKER order-ek
```

**Fontos**: A grid vonalak **fix pozíciókban** vannak. Amikor egy order teljesül, a counter order ára **mindig a szomszédos grid vonal előre kiszámított ára** – soha nem a fill árából újraszámolt érték. Ez biztosítja a konzisztenciát és az azonnali (~O(1)) reakciót.

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
  ├─ x=CANCELED + NEM volt helyi kérelem → EXTERNAL_CANCELED
  └─ x=REJECTED → REJECTED    (pl. PRICE_FILTER sértés)

FILLED → counter order küldés a szomszédos grid vonalra (grid_map lookup)
```

**Kulcsszabály:** A bot soha nem REST API-n kérdezi le az order állapotát. Kizárólag a Binance User Data Stream `executionReport` eseménye az igazság forrása.

---

## 2. Kódfolyamat – mi történik indításkor?

### `python src/app/main.py config.yaml`

A startup sorrend **kritikus**: a user data stream-nek **kötelezően fel kell épülnie** mielőtt bármilyen ordert nyitunk. Ha nincs user stream, a bot "vakon" kereskedne – nem tudná, mi történik az ordereivel.

```
app/main.py → main()
│
├─ 1. load_config("config.yaml")
│     └─ app/config.py: YAML beolvasás + Pydantic validáció
│
├─ 2. setup_logging()
│     └─ app/log_setup.py: structlog JSON formátum beállítás
│
├─ 3. init_db() + run_migrations()
│     └─ persistence/db.py: SQLAlchemy async engine
│        alembic upgrade head → 11 tábla létrehozás (ha kell)
│
├─ 4. Queue-k létrehozása (asyncio.Queue)
│     ws_send_queue  – WS küldendő parancsok
│     event_queue    – user stream események
│     db_queue       – DB írási feladatok
│     command_queue  – API parancsok (pause/stop/stb.)
│     broadcast_queue – frontend live events
│
├─ 5. Komponensek példányosítása
│     BinanceWsApi(config, ws_send_queue, db_queue)
│     UserDataStream(config, event_queue, ws_api=ws_api)
│     MarketStream(config, "SOLUSDT")
│     GridEngine(settings, ws_api, db_queue, event_queue, market_stream)
│     DbWriter(db_queue)
│     EmergencyStop(engine, ws_api, db_queue, safety_config)
│     Reconciliation(engine, ws_api, db_queue, safety_config)
│     Watchdog(engine, ws_api, user_stream, db_writer, emergency, safety_config)
│
├─ 6. asyncio.TaskGroup – MIND PÁRHUZAMOSAN INDUL:
│     ┌─ ws_api.writer_loop()     exchange/ws_api.py
│     ├─ ws_api.reader_loop()     exchange/ws_api.py
│     ├─ user_stream.start()      exchange/user_stream.py
│     ├─ market_stream.start()    exchange/market_stream.py
│     ├─ db_writer.run()          persistence/writer.py
│     ├─ event_dispatcher()       app/main.py
│     ├─ command_processor()      app/main.py
│     ├─ reconciliation.run()     supervisor/reconciliation.py
│     ├─ watchdog.run()           supervisor/watchdog.py
│     ├─ broadcast_loop()         api/live_events.py
│     └─ uvicorn.serve()          (FastAPI szerver)
│
├─ 7. ⏳ WAIT: ws_api._connected.wait()
│     └─ Megvárja amíg a Binance WS API WebSocket felépül
│        Timeout: 30s → RuntimeError ha nem sikerül
│
├─ 8. ⏳ WAIT: user_stream.connected.wait()
│     └─ Megvárja amíg a User Data Stream WebSocket felépül
│        AMÍG EZ NINCS, SEMMILYEN ORDERT NEM SZABAD NYITNI!
│        Timeout: 30s → RuntimeError ha nem sikerül
│
├─ 9. Open Orders lekérdezés
│     └─ ws_api.get_open_orders(symbol)
│        Szűrés: van-e "G-" prefixű (bot által nyitott) order?
│
├─ 10. DÖNTÉS:
│     ├─ HA vannak G- orderek → RECOVERY MÓD (_try_recovery)
│     │    └─ Korábbi grid újraépítése a bent ragadt orderekből + DB-ből
│     │
│     └─ HA nincsenek → FRISS INDÍTÁS
│          └─ bot_run létrehozás DB-ben + engine.initialize()
│
└─ 11. ⏳ WAIT: _shutdown.wait()
      └─ Futás amíg SIGINT/SIGTERM vagy API stop parancs nem jön
```

### Miért fontos a sorrend?

```
HELYTELEN (régi viselkedés):
  bot_run létrehozás → orderek küldése → user stream felépül
  ❌ Ha orderek teljesülnek mielőtt a user stream felépülne,
     a bot nem tudja meg → "elveszett" fill-ek → szétesik a grid

HELYES (jelenlegi viselkedés):
  WS API felépül → User stream felépül → Open orders check → AZTÁN döntés
  ✅ A bot MINDIG hallja az orderek állapotváltozásait
  ✅ Ha vannak bent ragadt orderek, recovery-vel folytatja
```

### `engine.initialize()` – a grid felépítés (friss indítás)

```
grid/engine.py → initialize()
│
├─ 0. Szerver idő szinkronizálás (WS API "time" hívás)
│     └─ exchange/signing.py: set_time_offset(server_ms - local_ms)
│        Ha a rendszeróra eltér, az HMAC aláírás érvénytelen → orderek elutasítva
│
├─ 1. ws_api.get_exchange_info("SOLUSDT")
│     └─ SymbolInfo objektum: tick_size, step_size, min_qty, min_notional, max_orders
│        Ezek kellenek a kerekítéshez – Binance visszautasítja ha nem tick_size/step_size-ra kerekítünk
│
├─ 2. order_quote_value számítás (ha nincs explicit megadva)
│     └─ auto: total_capital × (1 - reserve%) / max_levels
│        Validáció: legalább min_notional × 1.1 legyen (Binance minimum)
│
├─ 3. ws_api.get_account()
│     └─ inventory.update_from_account(): szabad + zárolt egyenleg beállítás
│
├─ 4. _determine_anchor_price()
│     └─ best_bid_ask_mid: market_stream.wait_for_price(timeout=15s)
│        bookTicker mid = (best_bid + best_ask) / 2 → EZ LESZ A GRID KÖZEPE
│
├─ 5. Grid lépés számítás (grid/calculator.py)
│     └─ Geometriai: r = (1 + fee_buy + profit/value) / (1 - fee_sell)
│        Aritmetikai: d = anchor × (r - 1) abszolút USDT-re
│        Biztonsági korlátok: min_grid_step_pct ≤ step ≤ max_grid_step_pct
│
├─ 6. K_buy, K_sell számítás
│     └─ available = total_capital × (1 - reserve%)
│        K_total = floor(available / order_quote_value)
│        K_buy = floor(K_total × buy_ratio), K_sell = K_total - K_buy
│
├─ 7. Grid szintek generálása (calculator.generate_geometric_grid)
│     └─ Buy: anchor / r^1, anchor / r^2, ..., anchor / r^K_buy
│        Sell: anchor × r^1, anchor × r^2, ..., anchor × r^K_sell
│        Minden ár: round_price_to_tick() → tick_size-ra kerekítve
│        Minden qty: round_down_to_step() → step_size-ra kerekítve
│        Validáció: min_qty, min_notional ellenőrzés
│
├─ 8. ⭐ Grid Map építés (dict[int, GridLevel])
│     └─ BUY szintek: grid_map[-1], grid_map[-2], ...
│        SELL szintek: grid_map[+1], grid_map[+2], ...
│        ANCHOR:       grid_map[0] = anchor_price
│        → O(1) counter order ár lookup fill után
│        RÉSZLETEK: lásd 3. fejezet
│
├─ 9. Grid paraméterek + szintek mentése DB-be (non-blocking)
│     └─ db_queue.put_nowait("update_bot_run_grid_params") → anchor_price, grid_step
│        db_queue.put_nowait("save_grid_levels") → grid_levels tábla
│        RÉSZLETEK: lásd 6. fejezet
│
└─ 10. _place_initial_orders() – ÖSSZES ORDER NON-BLOCKING
       └─ Minden grid szintre: _submit_level_order(level, cycle_id=0)
          → OrderRouter.submit_order(intent)
             → db_queue.put_nowait(DbEvent "upsert_order_from_intent")
             → ws_api.enqueue_order("order.place") → ws_send_queue
             → RETURN AZONNAL – nem vár semmire
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
1. listenKey megszerzése WS API-n (userDataStream.start) – NEM REST
2. WS csatlakozás: wss://stream.binance.com/ws/<listenKey>
3. Minden esemény → event_queue.put_nowait() – SEMMI MÁS NEM TÖRTÉNIK ITT
4. 30 percenként: listenKey megújítás WS API-n (userDataStream.ping)
5. Csatlakozás jelzés: self.connected.set() → asyncio.Event
   → main.py ebben bízik, hogy megvárja a kapcsolatot
```

**`event_dispatcher()`** `(app/main.py)`
```
event_queue-ból olvas:
  executionReport:
    → ExecutionReport.from_dict(data) – parse a raw dict-ből
    → state_machine.transition() – új order státusz meghatározása
    → db_queue: insert_execution_event (idempotens, execution_id UNIQUE)
    → db_queue: update_order_from_execution
    → Ha TRADE esemény: db_queue: insert_fill (idempotens, UNIQUE trade_id)
    → Ha FILLED: engine.on_execution_report() → counter order küldés
    → Ha EXTERNAL_CANCELED: engine.on_external_cancel() → policy alapján
  outboundAccountPosition:
    → inventory.update_from_account_position()
    → db_queue: update_balance (minden asset-re)
  balanceUpdate:
    → inventory.update_from_balance_update(asset, delta)
```

**`db_writer.run()`** `(persistence/writer.py)`
```
db_queue-ból olvas → PostgreSQL async write
14 különböző event típus kezelése (match/case):
  - upsert_order_from_intent: orders tábla (ON CONFLICT DO NOTHING)
  - update_order_from_execution: orders tábla frissítés
  - insert_fill: fills tábla (idempotens, uq_fill constraint)
  - insert_execution_event: execution_events tábla (idempotens)
  - update_balance: balances tábla
  - update_bot_status: bot_runs.status frissítés
  - update_bot_run_order_quote_value: bot_runs.order_quote_value
  - update_bot_run_grid_params: bot_runs anchor_price + grid_step mentés
  - save_grid_levels: grid_levels tábla bulk upsert
  - log_system_event: system_events tábla
  - log_external_event: external_events tábla
  - update_intent_state: order_intents.local_state frissítés
Ha DB nem elérhető: DEGRADED állapot, watchdog riaszt
```

**`watchdog.run()`** `(supervisor/watchdog.py)`
```
5 másodpercenként ellenőriz:
  - user_stream.last_event_age_sec > max_user_stream_staleness_sec? → emergency_stop
  - db_writer.queue_size > 90% of max? → emergency_stop
  - db_writer.is_degraded? → WARNING log
```

---

## 3. Grid Map – O(1) counter order lookup

### Mi a grid_map és miért kell?

A grid map egy `dict[int, GridLevel]` szótár, amely **index alapján** tárolja az összes grid szintet. Ez teszi lehetővé, hogy egy fill esemény után **milliszekundum alatt**, egyetlen dict lookup-pal megkapjuk a counter order árát.

**Miért nem az adatbázisból olvassuk?** Mert milliszekundumok számítanak. Ha a piac rángat, amíg a counter order bekerül az exchange-re, addig a piac elmehet – és az order beragad vagy rosszabb áron teljesül. A grid_map memóriában van, O(1) lookup, nulla I/O.

### Adatszerkezet

```python
# grid/engine.py – GridEngine.__init__()
self.grid_map: dict[int, GridLevel] = {}
```

```
Kulcs (index):  -4    -3    -2    -1     0      +1    +2    +3    +4
Érték (price): 159.65 162.19 164.75 167.33 [170.00] 170.45 173.10 175.80 178.50
                ────── BUY szintek ──────  ANCHOR  ────── SELL szintek ──────
```

**Index konvenció:**
- **Negatív index** = BUY szint (az anchor ALATT): -1, -2, -3, ...
- **Pozitív index** = SELL szint (az anchor FELETT): +1, +2, +3, ...
- **0** = Anchor (a grid közepe) – nem kerül rá order, de counter order célpont lehet

### Mikor épül a grid_map?

**Friss indítás** – `engine.initialize()` (`src/grid/engine.py:231-240`):

```python
# Grid szintek generálása UTÁN (calculator.generate_geometric_grid):
self.grid_map = {}
for level in self.grid_plan.buy_levels:
    self.grid_map[level.index] = level        # grid_map[-1], [-2], ...
for level in self.grid_plan.sell_levels:
    self.grid_map[level.index] = level        # grid_map[+1], [+2], ...
self.grid_map[0] = GridLevel(                 # Anchor szint
    index=0, price=anchor_price, side="ANCHOR",
    quantity=Decimal("0"), notional=Decimal("0"), zone="ANCHOR",
)
```

**Recovery** – `engine.recover()` (`src/grid/engine.py:584-603`):

```python
# DB-ből betöltött grid_levels-ből:
self.grid_map = {}
for gl in grid_levels_db:
    side = "BUY" if gl.level_index < 0 else ("SELL" if gl.level_index > 0 else "ANCHOR")
    level = GridLevel(
        index=gl.level_index, price=gl.price, side=side,
        quantity=Decimal("0"), notional=Decimal("0"), zone=gl.side_zone,
    )
    self.grid_map[gl.level_index] = level
```

### GridLevel adatszerkezet

```python
# grid/calculator.py
@dataclass
class GridLevel:
    index: int         # -3, -2, -1, 0, +1, +2, +3
    price: Decimal     # Binance tick_size-ra kerekített ár
    side: str          # "BUY" | "SELL" | "ANCHOR"
    quantity: Decimal   # step_size-ra kerekített mennyiség
    notional: Decimal   # price × quantity (USDT értékben)
    zone: str          # "BELOW_ANCHOR" | "ABOVE_ANCHOR" | "ANCHOR"
```

---

## 4. Counter Order logika – fill utáni azonnali reakció

### A teljes fill → counter order folyamat

```
1. Binance: BUY order teljesül a -2. szinten (164.75 USDT)
   ↓
2. User Data Stream: executionReport event (x=TRADE, X=FILLED)
   ↓  exchange/user_stream.py: event_queue.put_nowait(data)
   ↓
3. event_dispatcher(): event_queue-ból kiolvasás
   ↓  app/main.py: ExecutionReport.from_dict(data) parse
   ↓  state machine: SUBMITTED_UNKNOWN → FILLED
   ↓  DB queue: execution_event + order update + fill mentés
   ↓  engine.on_execution_report(report) hívás
   ↓
4. engine._on_order_filled(report)
   ↓  level_index = self._order_levels[cid]  → -2
   ↓  side = "BUY" → counter_index = -2 + 1 = -1
   ↓
5. grid_map lookup: grid_line = self.grid_map[-1]
   ↓  counter_price = 167.33  ← O(1), fix ár, NINCS újraszámolás!
   ↓
6. Quantity számítás:
   ↓  BUY fill → SELL counter: qty = filled_qty - commission (ha base-ben)
   ↓  qty = round_down_to_step(qty, step_size)
   ↓
7. _submit_level_order(counter_level, cycle_id)
   ↓  OrderRouter.submit_order(intent)
   ↓  → db_queue.put_nowait() → ws_send_queue.put_nowait()
   ↓  → RETURN AZONNAL
   ↓
8. Binance: SELL order a -1. szinten (167.33 USDT) benn van a könyvben
```

### A kód részletesen

**`engine._on_order_filled()`** (`src/grid/engine.py:377-434`):

```python
async def _on_order_filled(self, report: ExecutionReport) -> None:
    # 1. Csak RUNNING állapotban reagálunk
    if self.status != BotStatus.RUNNING:
        return

    cid = report.client_order_id
    level_index = self._order_levels.get(cid)  # melyik szinten volt?
    side = self._order_sides.get(cid)          # BUY vagy SELL volt?

    if level_index is None or side is None:
        log.warning("Ismeretlen level a fill-hez", cid=cid)
        return

    self._cycle_seq += 1  # ciklus számláló növelés

    # 2. Counter irány és quantity meghatározás
    if side == "BUY":
        counter_index = level_index + 1    # BUY fill → SELL counter (eggyel feljebb)
        counter_side = "SELL"
        qty = report.cumulative_filled_qty  # kapott base mennyiség
        if report.commission_asset == self.settings.bot.base_asset:
            qty -= report.commission_amount  # ha SOL-ban volt a jutalék, levonjuk
        qty = round_down_to_step(qty, self.symbol_info.lot_size.step_size)
    else:  # SELL fill
        counter_index = level_index - 1    # SELL fill → BUY counter (eggyel lejjebb)
        counter_side = "BUY"
        qty = Decimal("0")                 # BUY qty-t az ár alapján számoljuk

    # 3. Grid map lookup – O(1), FIX ÁR
    grid_line = self.grid_map.get(counter_index)
    if grid_line is None:
        log.error("Nincs grid vonal", counter_index=counter_index)
        return  # szélső szint fill → nincs további grid vonal
    counter_price = grid_line.price

    # 4. BUY counter qty: order_quote_value / ár
    if side == "SELL":
        qty = round_down_to_step(
            self.settings.bot.order_quote_value / counter_price,
            self.symbol_info.lot_size.step_size,
        )

    # 5. Counter order beküldés (non-blocking)
    counter_level = GridLevel(
        index=counter_index, price=counter_price, side=counter_side,
        quantity=qty, notional=qty * counter_price,
        zone="ABOVE_ANCHOR" if counter_price > self.grid_map[0].price else "BELOW_ANCHOR",
    )
    self._submit_level_order(counter_level, cycle_id=self._cycle_seq)
```

### Miért NEM számolunk fill_price × (1 + step)?

```
HELYTELEN (régi logika):
  BUY fill 164.50 USDT-on (ami pont nem a grid vonal)
  → counter ár = 164.50 × 1.016 = 167.13 USDT
  ❌ Ez NEM a grid vonal! Idővel a grid szétcsúszik.

HELYES (jelenlegi logika):
  BUY fill a -2. szinten → counter = grid_map[-1].price = 167.33 USDT
  ✅ Mindig a fix, előre kiszámított grid vonalra megy
  ✅ A grid konzisztens marad bármeddig fut
```

### Quantity számítás részletek

**BUY fill → SELL counter:**
```
qty = report.cumulative_filled_qty         # mennyi SOL-t kaptunk
    - report.commission_amount             # mínusz jutalék (ha SOL-ban fizettük)
qty = round_down_to_step(qty, step_size)   # Binance step_size-ra kerekítés
```

**SELL fill → BUY counter:**
```
qty = order_quote_value / counter_price    # hány SOL-t vesz az adott áron
qty = round_down_to_step(qty, step_size)   # Binance step_size-ra kerekítés
```

### In-memory order tracking

A grid engine három szótárral követi nyomon melyik order melyik szinten van:

```python
# grid/engine.py – GridEngine.__init__()
self._level_orders: dict[int, str] = {}   # level_index → client_order_id
self._order_levels: dict[str, int] = {}   # client_order_id → level_index
self._order_sides:  dict[str, str] = {}   # client_order_id → "BUY"|"SELL"
```

Új order beküldésekor (`_submit_level_order`):
```python
self._level_orders[level.index] = cid   # pl. -2 → "G-000001-B-002-0001-01"
self._order_levels[cid] = level.index   # inverz mapping
self._order_sides[cid] = level.side     # "BUY"
```

Fill beérkezésekor (`_on_order_filled`):
```python
level_index = self._order_levels[cid]   # a CID-ből visszakeressük a szintet
side = self._order_sides[cid]           # és az irányt
```

---

## 5. Recovery – újraindulás bent ragadt orderekből

### Mikor van szükség recovery-re?

Ha a bot leáll (crash, kill -9, Docker restart, OOM) és az orderek bent maradnak az exchange-en:

```
1. Bot fut, 8 order az exchange-en (4 BUY + 4 SELL)
2. Kill -9 / Docker restart / crash
3. Bot újraindul → open_orders lekérdezés → talál 8 db "G-" prefixű ordert
4. RECOVERY MÓD: újraépíti az állapotot, NEM hoz létre új bot_run-t
```

### A recovery teljes folyamata

```
main.py → main()
│
├─ WAIT: ws_api._connected.wait()         ← WS API felépül
├─ WAIT: user_stream.connected.wait()     ← User stream felépül
│     ↑ EDDIG NEM SZABAD SEMMIT CSINÁLNI ↑
│
├─ ws_api.get_open_orders(symbol)
│     └─ Szűrés: clientOrderId.startswith("G-")
│
├─ HA vannak G- orderek → _try_recovery() hívás
│     │
│     ├─ 1. clientOrderId parse: "G-000042-B-002-0003-05"
│     │                            │  │      │  │    │    │
│     │                            │  │      │  │    │    └─ seq
│     │                            │  │      │  │    └────── cycle
│     │                            │  │      │  └─────────── level (abs)
│     │                            │  │      └────────────── B=BUY / S=SELL
│     │                            │  └───────────────────── run short_id (base36)
│     │                            └──────────────────────── "G" prefix
│     │
│     ├─ 2. parse_run_short_id("000042") → bot_run.id = 150
│     │     └─ Base36 dekódolás: "000042" → 4×36 + 2 = 150
│     │
│     ├─ 3. DB ellenőrzés:
│     │     ├─ BotRunRepo.get(150) → bot_run rekord
│     │     │   └─ Ha nincs → cancel all, friss indítás
│     │     ├─ bot_run.symbol == config.symbol?
│     │     │   └─ Ha nem → cancel all, friss indítás (config megváltozott)
│     │     └─ GridLevelRepo.load_by_run(150) → grid szintek a DB-ből
│     │         └─ Ha üres → cancel all, friss indítás (crash mentés előtt volt)
│     │
│     ├─ 4. exchangeInfo lekérés → SymbolInfo (precision filterek)
│     │
│     └─ 5. engine.recover() hívás
│
└─ HA nincsenek → friss indítás (bot_run + initialize)
```

### `engine.recover()` – részletes lépések

**Fájl:** `src/grid/engine.py:548-668`

```
engine.recover(bot_run_id, bot_run_short_id, bot_run_record,
               grid_levels_db, open_orders, symbol_info)
│
├─ R1: Alapok beállítása
│     bot_run_id, _bot_run_short_id, symbol_info mentés
│     OrderRouter példányosítás (ws_api, db_queue, bot_run_id)
│
├─ R2: Szerver idő szinkronizálás
│     ws_api._query("time") → set_time_offset()
│     Nélküle az HMAC aláírás érvénytelen
│
├─ R3: Grid map újraépítés DB-ből
│     grid_levels tábla → self.grid_map dict feltöltés
│     Ugyanaz a dict[int, GridLevel] mint friss indításkor
│     Pl: grid_map[-3] = GridLevel(index=-3, price=162.19, side="BUY", ...)
│
├─ R4: GridPlan visszaállítás
│     bot_run_record-ból: grid_type, anchor_price, grid_step_pct/abs
│     buy_levels + sell_levels a grid_map-ból
│     order_quote_value visszaállítás a config-ba
│
├─ R5: In-memory tracking újraépítés open orderekből
│     Minden "G-" prefixű open order-ből:
│       cid parse → side (B/S), level_index (abs → signed)
│       self._level_orders[level_index] = cid
│       self._order_levels[cid] = level_index
│       self._order_sides[cid] = side
│       self.router._submitted.add(cid)  ← dupla submit védelem
│     BOOT prefixű order → bootstrap_pending = True
│
├─ R6: Sequence counters visszaállítás
│     Megkeresi a legmagasabb cycle_id és seq értékeket
│     az open order CID-ekből → self._cycle_seq, self._order_seq
│     Így az új orderek nem ütköznek a régiekkel
│
├─ R7: Account frissítés
│     ws_api.get_account() → inventory.update_from_account()
│
└─ R8: Status = RUNNING
      A bot fut tovább, mintha mi sem történt volna
      A user stream már figyel → fill esetén counter order megy
```

### clientOrderId formátum és parse

**Generálás** (`grid/order_router.py:make_client_order_id`):
```python
# G-{run6}-{S1}-{lvl3}-{cyc4}-{sq2}
cid = f"G-{bot_run_short_id}-{s}-{abs(level_index):03d}-{cycle_id:04d}-{seq:02d}"
# Példa: "G-00004m-B-003-0012-07"
```

**Base36 kódolás** (`app/main.py:make_run_short_id`):
```python
def make_run_short_id(run_id: int) -> str:
    chars = "0123456789abcdefghijklmnopqrstuvwxyz"
    n = run_id
    result = []
    for _ in range(6):
        result.append(chars[n % 36])
        n //= 36
    return "".join(reversed(result))
# 150 → "00004m", 1 → "000001", 46655 → "000zzz"
```

**Base36 dekódolás** (`app/main.py:parse_run_short_id`):
```python
def parse_run_short_id(short: str) -> int:
    chars = "0123456789abcdefghijklmnopqrstuvwxyz"
    n = 0
    for c in short:
        n = n * 36 + chars.index(c)
    return n
# "00004m" → 150, "000001" → 1, "000zzz" → 46655
```

### Szélső esetek kezelése

| Eset | Mi történik | Kód helye |
|------|------------|-----------|
| **Graceful shutdown** (nincs bent order) | Friss indítás – normális viselkedés | main.py:473 |
| **Crash/kill -9** (orderek bent) | Recovery: grid_map + tracking DB-ből + open orderekből | main.py:470 |
| **Config symbol megváltozott** | Cancel all, friss indítás + warning log | main.py:337 |
| **Grid levels nincs DB-ben** (crash mentés előtt) | Cancel all, friss indítás + warning log | main.py:345 |
| **bot_run nem található DB-ben** | Cancel all, friss indítás | main.py:332 |
| **Bootstrap order bent ragadt** | `_bootstrap_pending = True`, várja a fill-t | engine.py:632 |
| **Több run short_id az orderek közt** | Az első order run_id-ját használja | main.py:318 |

---

## 6. DB Persistence – grid szintek mentése

### Miért kell DB-be is menteni a grid-et?

A `grid_map` memóriában van és a bot leállásakor elvész. A DB persistence biztosítja, hogy:
1. **Recovery** lehetséges legyen (a grid szinteket a DB-ből töltjük vissza)
2. **Audit trail**: utólag is látható milyen grid szintekkel futott a bot
3. **API**: az API végpontokon is lekérdezhető a grid

### Mentés folyamata

**Időzítés:** `engine.initialize()` → grid generálás UTÁN, orderek ELŐTT

```
engine.initialize()
│
├─ Grid szintek kiszámolva (calculator)
├─ grid_map feltöltve (memória)
│
├─ db_queue.put_nowait("update_bot_run_grid_params")
│     → DbWriter → SQL UPDATE bot_runs SET anchor_price=..., grid_step_pct=...
│     WHERE id = bot_run_id
│
├─ db_queue.put_nowait("save_grid_levels")
│     → DbWriter → GridLevelRepo.bulk_upsert()
│        → Minden szintre: INSERT INTO grid_levels (bot_run_id, level_index, price, side_zone)
│          ON CONFLICT (bot_run_id, level_index) DO NOTHING
│
└─ _place_initial_orders()  ← ezek UTÁN mennek ki az orderek
```

### Érintett DB táblák

**`bot_runs` tábla** – grid paraméterek:
```sql
-- Friss indításkor mentett mezők (korábban üresen maradtak!):
anchor_price    NUMERIC(24,8)  -- a grid közepe (pl. 170.00000000)
grid_step_pct   NUMERIC(10,8)  -- geometriai lépés % (pl. 0.01630000)
grid_step_abs   NUMERIC(24,8)  -- aritmetikai lépés USDT (pl. 2.77000000)
order_quote_value NUMERIC(24,8) -- egy order értéke USDT-ben (pl. 5.00000000)
```

**`grid_levels` tábla** – szintenként egy sor:
```sql
CREATE TABLE grid_levels (
    id          SERIAL PRIMARY KEY,
    bot_run_id  INTEGER NOT NULL,
    level_index INTEGER NOT NULL,     -- -4,-3,...,0,...,+3,+4
    price       NUMERIC(24,8) NOT NULL, -- tick_size-ra kerekített ár
    side_zone   VARCHAR(20) NOT NULL,  -- BELOW_ANCHOR | ABOVE_ANCHOR | ANCHOR
    created_at  TIMESTAMPTZ DEFAULT now(),
    UNIQUE (bot_run_id, level_index)   -- constraint: uq_grid_level
);
```

**Példa adat:**
```
bot_run_id | level_index | price        | side_zone
-----------|-------------|-------------|------------
42         | -4          | 159.65000000 | BELOW_ANCHOR
42         | -3          | 162.19000000 | BELOW_ANCHOR
42         | -2          | 164.75000000 | BELOW_ANCHOR
42         | -1          | 167.33000000 | BELOW_ANCHOR
42         |  0          | 170.00000000 | ANCHOR
42         | +1          | 170.45000000 | ABOVE_ANCHOR
42         | +2          | 173.10000000 | ABOVE_ANCHOR
42         | +3          | 175.80000000 | ABOVE_ANCHOR
42         | +4          | 178.50000000 | ABOVE_ANCHOR
```

### DbWriter event handler-ek

**`save_grid_levels`** (`src/persistence/writer.py:119-124`):
```python
case "save_grid_levels":
    repo = GridLevelRepo(session)
    await repo.bulk_upsert(
        event.data["bot_run_id"],
        event.data["levels"],
    )
```

**`update_bot_run_grid_params`** (`src/persistence/writer.py:105-117`):
```python
case "update_bot_run_grid_params":
    await session.execute(
        sa_update(BotRun).where(BotRun.id == event.data["run_id"]).values(
            anchor_price=event.data["anchor_price"],
            grid_step_pct=event.data.get("grid_step_pct"),
            grid_step_abs=event.data.get("grid_step_abs"),
            order_quote_value=event.data["order_quote_value"],
        )
    )
```

### GridLevelRepo

**Fájl:** `src/persistence/repositories.py:170-191`

```python
class GridLevelRepo:
    async def bulk_upsert(self, bot_run_id: int, levels: list[dict]) -> None:
        """Idempotens grid szint mentés – ON CONFLICT DO NOTHING."""
        for level_data in levels:
            stmt = insert(GridLevel).values(
                bot_run_id=bot_run_id,
                level_index=level_data["level_index"],
                price=level_data["price"],
                side_zone=level_data["side_zone"],
            ).on_conflict_do_nothing(constraint="uq_grid_level")
            await self.session.execute(stmt)

    async def load_by_run(self, bot_run_id: int) -> list[GridLevel]:
        """Recovery-hez: grid szintek betöltése egy bot run-hoz."""
        result = await self.session.execute(
            select(GridLevel)
            .where(GridLevel.bot_run_id == bot_run_id)
            .order_by(GridLevel.level_index)
        )
        return list(result.scalars().all())
```

### Adat áramlás összefoglaló

```
FRISS INDÍTÁS:
  calculator → grid_plan → grid_map (memória) → db_queue → DbWriter → PostgreSQL
                                                                         ↓
                                                                    grid_levels tábla
                                                                    bot_runs tábla

RECOVERY:
  PostgreSQL → GridLevelRepo.load_by_run() → grid_map (memória)
  PostgreSQL → BotRunRepo.get() → grid_plan (memória)
  Exchange open orders → _level_orders, _order_levels, _order_sides (memória)
```

---

## 7. Konfiguráció – minden mező magyarázata

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
  # A bot által kezelt összes USDT. Ebből számítja a rendszer,
  # mennyi jut egy grid vonalra (ha order_quote_value nincs megadva).
  
  # order_quote_value: "2.5"
  # OPCIONÁLIS – ha kihagyod, a rendszer AUTOMATIKUSAN kiszámolja:
  #     order_quote_value = total_capital × (1 - tartalék%) / max_grid_levels
  #     Példa: 50 × 0.98 / 20 = 2.45 USDT / szint
  # Validáció: legalább Binance min_notional × 1.1 kell legyen
  
  target_net_profit_per_cycle_quote: "0.02"
  # Minimálisan elvárt NETTÓ profit egy buy-sell körön (USDT).
  # Ebből számolja a rendszer a lépésközt:
  #   GEOMETRIC: r = (1 + fee_buy + profit/order_value) / (1 - fee_sell)
  #   ARITHMETIC: d = anchor_price × (r - 1)
  
  grid_type: geometric
  # "geometric":  lépés %-os → minden szinten AZONOS profit/ciklus
  # "arithmetic": lépés fix USDT → profit/ciklus szintenként VÁLTOZIK
  
  inventory_mode: prebalanced
  # "prebalanced":          van meglévő SOL-od, azt használja sell order-ekhez
  # "use_existing_balances": hasonló
  # "quote_only_bootstrap": csak USDT-d van → először vásárol SOL-t
  
  buy_allocation_ratio: "0.5"
  # A tőke mekkora hányada megy buy szintekre vs sell szintekre.
  
  quote_reserve_pct: "0.02"
  # A USDT egyenleg hány %-át NE helyezze orderbe (tartalék).
  
  base_reserve_pct: "0.02"
  # A base eszköz egyenleg hány %-át NE helyezze orderbe.
  
  order_type: LIMIT_MAKER
  # "LIMIT_MAKER": post-only order – csak maker díjat fizet
  #   NINCS timeInForce paraméter! (Binance szabály)
  # "LIMIT": hagyományos limit order – teljesülhet taker-ként is
  
  time_in_force: GTC        # Good Till Canceled
  
  max_grid_levels: 20
  # Maximum hány grid szint lehet összesen (buy + sell).
  
  min_grid_step_pct: "0.0025"
  # ALSÓ BIZTONSÁGI KORLÁT – a tényleges lépést a bot SZÁMÍTJA.
  
  max_grid_step_pct: "0.05"
  # FELSŐ BIZTONSÁGI KORLÁT – elírás védelem.
```

### `fees` – trading díjak

```yaml
fees:
  fee_mode: maker_assumed
  maker_fee_buy: "0.001"    # 0.1% – Binance standard
  maker_fee_sell: "0.001"
  taker_fee_buy: "0.001"
  taker_fee_sell: "0.001"
```

A díjakat a bot arra használja, hogy **kiszámolja a minimálisan szükséges grid lépést**, ami felett a ciklus profitábilis.

### `bootstrap` – kezdeti SOL vásárlás

```yaml
bootstrap:
  order_type: MARKET        # MARKET | LIMIT | LIMIT_MAKER
  # quote_qty: "25"         # Opcionális, default: total_capital × buy_ratio
  limit_price_offset_pct: "0.001"
```

### `anchor` – a grid közepe

```yaml
anchor:
  source: best_bid_ask_mid  # "best_bid_ask_mid" | "last_trade" | "manual"
  # manual_price: "170"     # csak source: manual esetén
```

### `safety` – biztonsági korlátok

```yaml
safety:
  external_intervention_policy: pause
  # "pause":              bot szünetel ha kézzel törölnek ordert
  # "continue_reconcile": fut tovább
  # "emergency_stop":     azonnal leáll
  
  max_user_stream_staleness_sec: 10
  max_trading_ws_staleness_sec: 10
  cancel_retry_interval_sec: 2
  cancel_retry_max: 5
  emergency_stop_on_db_queue_full: true
  emergency_stop_on_balance_mismatch: true
  sell_on_emergency_stop: false
```

### `database` – adatbázis

```yaml
database:
  host: localhost
  port: 5432
  name: kebo_db
  user: kebo_grid
  password_env: DATABASE_PASSWORD
  pool_min_size: 1
  pool_max_size: 10
  writer_queue_max_size: 10000
```

---

## 8. Adatbázis – táblák és migrációk

**A táblákat nem kell kézzel létrehozni.** A bot indításkor automatikusan futtatja az Alembic migrációkat.

| Tábla | Mire való | Sorok típusa |
|-------|-----------|-------------|
| `bot_runs` | Minden bot indítás rögzítve, konfig snapshot, grid params | 1 sor / indítás |
| `grid_levels` | A generált grid szintek árai (recovery-hez!) | K_buy + K_sell + 1 sor / run |
| `order_intents` | Beküldési szándékok (mielőtt Binance-re megy) | 1 sor / order |
| `orders` | Exchange oldalon lévő order-ek és állapotuk | 1 sor / order |
| `fills` | Kötések – UNIQUE(symbol, order_id, trade_id) | 1 sor / fill |
| `execution_events` | Összes Binance executionReport nyers formában | 1+ sor / order |
| `balances` | Egyenleg változások időbélyeggel | folyamatos |
| `external_events` | Kézi beavatkozások (pl. kézzel törölt order) | ritka |
| `ws_connections` | WebSocket kapcsolat log | csatlakozásonként |
| `system_events` | Rendszer alertek, watchdog események | riasztásonként |
| `api_audit` | API hívások naplója | hívásonként |

### A `clientOrderId` is tartalmazza a bot azonosítóját

```
G-000001-B-001-0000-01
│       │   │   │   └─ sorszám (00-99)
│       │   │   └───── ciklus szám (0000-9999)
│       │   └─────────── grid szint abszolút értéke (001-999)
│       └─────────────── B=BUY / S=SELL / BOOT=bootstrap
└─────────────────────── bot_run_id base36 kódolva (6 karakter)
```

### Lekérdezés SQL-ben

```sql
-- Melyik botok futnak éppen?
SELECT id, symbol, status, anchor_price, grid_step_pct FROM bot_runs WHERE status='RUNNING';

-- Egy adott bot grid szintjei (recovery-hez is ez a forrás)
SELECT level_index, price, side_zone FROM grid_levels WHERE bot_run_id = 42 ORDER BY level_index;

-- Egy adott bot összes ordere
SELECT client_order_id, side, price, status_exchange, is_working FROM orders WHERE bot_run_id = 42;

-- Összesített P&L bot_run_id szerint
SELECT bot_run_id, SUM(quote_quantity - commission_amount) FROM fills GROUP BY bot_run_id;
```

---

## 9. Telepítés és futtatás

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
source .env
python src/app/main.py configs/solusdt.yaml
```

### Docker

```bash
cp .env.example .env                                   # töltsd ki
cp configs/solusdt.example.yaml configs/solusdt.yaml  # szerkeszd
docker-compose up -d bot-solusdt
docker-compose logs -f bot-solusdt
```

### Recovery tesztelés

```bash
# 1. Bot indítás – orderek felkerülnek az exchange-re
docker-compose up -d bot-solusdt

# 2. Kényszerített leállítás (orderek bent maradnak)
docker-compose stop bot-solusdt          # graceful: cancel all, friss indítás lesz
# VAGY:
docker kill kebo-grid-bot-solusdt-1      # kill -9: orderek bent maradnak → RECOVERY

# 3. Újraindítás → log-ban "Recovery" jelenik meg
docker-compose up -d bot-solusdt
docker-compose logs -f bot-solusdt | grep -i recovery
```

**Elvárt log recovery esetén:**
```
Recovery jelölt  run_id=42  short_id=000016  open_bot_orders=8
Szerver idő szinkronizálva (recovery)  offset_ms=23
Recovery kész  bot_run_id=42  open_orders=8  grid_levels=9
```

---

## 10. Több pár párhuzamosan

Minden pár egy külön Docker konténerben fut, saját konfiggal és saját API porttal.

```bash
cp configs/solusdt.example.yaml configs/solusdt.yaml  # port: 8081
cp configs/btcusdt.example.yaml configs/btcusdt.yaml  # port: 8082

docker-compose up -d bot-solusdt bot-btcusdt
```

Minden bot teljesen független: saját egyenleg, saját grid, saját bot_run_id, saját recovery.

---

## 11. API végpontok

Alap URL: `http://localhost:8080`

| Metódus | Végpont | Leírás |
|---------|---------|--------|
| GET | `/health` | Él-e a szerver? |
| GET | `/status` | Bot státusz, grid szintek, open orders száma |
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

## 12. Tesztek

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

## Architektúra összefoglaló – adat áramlás

```
┌─────────────────────────────────────────────────────────────────┐
│                        BINANCE EXCHANGE                         │
│  ┌─────────────┐  ┌───────────────────┐  ┌─────────────────┐  │
│  │  WS API      │  │  User Data Stream │  │  Market Stream  │  │
│  │  order.place  │  │  executionReport  │  │  bookTicker     │  │
│  │  cancelAll    │  │  balanceUpdate    │  │  (bid/ask)      │  │
│  └──────┬───────┘  └────────┬──────────┘  └───────┬─────────┘  │
│         │                   │                      │            │
└─────────┼───────────────────┼──────────────────────┼────────────┘
          │                   │                      │
          ▼                   ▼                      ▼
   ws_send_queue        event_queue           market_stream
          │                   │                      │
          │                   ▼                      │
          │          event_dispatcher()              │
          │            │           │                  │
          │            ▼           ▼                  │
          │      state_machine   engine               │
          │                   .on_execution_report()  │
          │                        │                  │
          │                        ▼                  │
          │              _on_order_filled()           │
          │                        │                  │
          │           ┌────────────┤                  │
          │           │            │                  │
          │           ▼            ▼                  │
          │      grid_map      _submit_level_order()  │
          │      O(1)             │                    │
          │      lookup           │                    │
          │                       │                    │
          │         ┌─────────────┤                    │
          │         │             │                    │
          ▼         ▼             ▼                    │
   ┌──────────┐  ┌──────────┐                         │
   │ WS API   │  │ db_queue │                         │
   │ writer   │  │          │                         │
   │ loop     │  └────┬─────┘                         │
   └──────────┘       │                               │
                      ▼                               │
                  DbWriter                            │
                      │                               │
                      ▼                               │
              ┌──────────────┐                        │
              │  PostgreSQL  │                        │
              │  bot_runs    │◄── recovery ────────────┘
              │  grid_levels │    (DB → grid_map)
              │  orders      │
              │  fills       │
              │  ...         │
              └──────────────┘
```

### Kulcs tervezési döntések

1. **Nincs REST API hívás** – minden kommunikáció WebSocket-en. A listenKey sem REST-en jön.
2. **Non-blocking orderek** – submit_order() AZONNAL visszatér, nem vár ACK-ra.
3. **User Data Stream = igazság forrása** – az order státusza KIZÁRÓLAG az executionReport-ból jön.
4. **O(1) counter order** – grid_map dict lookup, nulla DB I/O a hot path-ban.
5. **Idempotens DB írás** – ON CONFLICT DO NOTHING, dupla event nem dupláz.
6. **Recovery képesség** – bent ragadt orderekből + DB grid_levels-ből újraépül a teljes állapot.
7. **Startup sorrend garancia** – user stream KELL hogy felépüljön mielőtt bármit nyitunk.

---

## Figyelmeztetések

- **Soha ne commitold a `.env` fájlt vagy az éles `configs/*.yaml` fájlokat!**
- A `.gitignore` automatikusan kizárja ezeket.
- **Testnet-en tesztelj először** – a valódi pénz elveszhet.
- A `LIMIT_MAKER` order visszautasítódik (EXPIRED), ha azonnal teljesülne. Ez normális – a bot nem generál counter order-t EXPIRED esetén.
- A `LIMIT_MAKER` order típusnál **NINCS timeInForce paraméter** – a Binance visszautasítja ha küldünk egyet.
