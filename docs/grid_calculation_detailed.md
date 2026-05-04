# Grid Kalkuláció — Részletes Leírás Példával

## Tartalomjegyzék

1. [Fogalmak](#1-fogalmak)
2. [Tőzsdei megkötések (Exchange Filters)](#2-tőzsdei-megkötések-exchange-filters)
3. [Geometriai Grid](#3-geometriai-grid)
4. [Aritmetikai Grid](#4-aritmetikai-grid)
5. [Grid Count (k_buy, k_sell) számítás](#5-grid-count-számítás)
6. [total_capital_quote és inventory_mode](#6-total_capital_quote-és-inventory_mode)
7. [Teljes példa: SOLUSDC geometriai grid](#7-teljes-példa-solusdc-geometriai-grid)

---

## 1. Fogalmak

| Fogalom | Jelentés |
|---|---|
| **anchor_price** | Referencia ár — a grid ezen ár körül épül fel. Forrás: live market mid price, vagy manual. |
| **order_quote_value** | Egy grid vonal quote (USDC) értéke. Pl. 5.46 USDC. |
| **grid_step** | Két szomszédos grid vonal közötti távolság (%-ban vagy abszolút). |
| **k_buy** | Buy szintek száma (anchor ALATT). |
| **k_sell** | Sell szintek száma (anchor FÖLÖTT). |
| **break-even step** | A minimális lépésköz, aminél a ciklus NULLÁS (fee-t épp fedezi). |
| **half-match** | Egy fill (buy vagy sell) ami még nincs párosítva ellentétes oldallal. |
| **base asset** | A kereskedett eszköz (pl. SOL). |
| **quote asset** | Az ár-denominációs eszköz (pl. USDC). |

---

## 2. Tőzsdei megkötések (Exchange Filters)

A Binance `exchangeInfo` 3 kritikus filtert tartalmaz, amiket MINDEN order-nek teljesítenie kell:

### PRICE_FILTER
```
minPrice  ≤  price  ≤  maxPrice
(price - minPrice) % tickSize == 0
```
- **tick_size**: az ár legkisebb lépésköze
- Példa SOLUSDC: `tick_size = 0.01` → ár csak 85.27, 85.28, ... lehet, 85.275 NEM

**Kezelés a kódban:** `round_price_to_tick(price, tick_size)` — legközelebbi tick-re kerekít.
- Fájl: `src/exchange/precision.py:12`

### LOT_SIZE
```
minQty  ≤  quantity  ≤  maxQty
(quantity - minQty) % stepSize == 0
```
- **step_size**: a mennyiség legkisebb lépésköze
- Példa SOLUSDC: `step_size = 0.001` → qty csak 0.063, 0.064, ... lehet

**Kezelés:** `round_down_to_step(qty, step_size)` — LEFELÉ kerekít (sosem kér többet, mint ami van).
- Fájl: `src/exchange/precision.py:5`

### NOTIONAL (MIN_NOTIONAL)
```
price × quantity  ≥  minNotional
```
- Példa SOLUSDC: `min_notional = 5.0` → 85.27 × 0.064 = 5.457 ✓

**Kezelés:** `validate_order()` ellenőrzi. Ha nem teljesül, az a grid level kihagyásra kerül.
- Fájl: `src/exchange/filters.py:23`

### Összefoglalás: egy order érvényes ha

```
price   = round_price_to_tick(raw_price, tick_size)     → 85.27
qty     = round_down_to_step(raw_qty, step_size)        → 0.064
notional = price × qty                                  → 5.457 ≥ 5.0 ✓
```

---

## 3. Geometriai Grid

### Lépésköz számítás

A geometriai gridnél a szintek **szorzóval** vannak elosztva: minden szint az előző szint × r.

```
r = 1 + grid_step_pct
```

A `grid_step_pct` a profitcélból és díjakból számolódik:

```
Egy ciklus költsége:     cost    = order_quote_value × (1 + fee_buy)
Egy ciklus bevétele:     revenue = order_quote_value × (1 + step) × (1 - fee_sell)
Profitábilis ha:         revenue > cost
Tehát:                   (1 + step) × (1 - fee_sell) > (1 + fee_buy)
Minimális step:          step_breakeven = (1 + fee_buy) / (1 - fee_sell) - 1
```

**Target profit-tal:**
```
revenue = cost + target_profit
order_quote_value × (1 + step) × (1 - fee_sell) = order_quote_value × (1 + fee_buy) + target_profit
(1 + step) = (1 + fee_buy + target_profit / order_quote_value) / (1 - fee_sell)
step = (1 + fee_buy + target_profit / order_quote_value) / (1 - fee_sell) - 1
```

**Hard floor:** A kód MINDIG érvényesíti:
```python
effective_min = max(min_step_pct, break_even_step)
g = max(g, effective_min)
```

**Tehát a grid SOHA nem lehet veszteséges, bármit is állít be a user a konfigban.**

Fájl: `src/grid/calculator.py:37-68`

### Szintek generálása

```
Anchor price: P₀

BUY szintek (anchor ALATT):
  Level -1: P₀ / r¹     → pl. 85.46 / 1.002 = 85.289 → kerekítve 85.29
  Level -2: P₀ / r²     → pl. 85.46 / 1.004 = 85.118 → kerekítve 85.12
  Level -3: P₀ / r³     → stb.

SELL szintek (anchor FÖLÖTT):
  Level +1: P₀ × r¹     → pl. 85.46 × 1.002 = 85.631 → kerekítve 85.63
  Level +2: P₀ × r²     → pl. 85.46 × 1.004 = 85.802 → kerekítve 85.80
  Level +3: P₀ × r³     → stb.
```

**BUY szintek mennyisége:**
```python
raw_qty = order_quote_value / price
qty = round_down_to_step(raw_qty, step_size)
```
Minden BUY szint más qty-t kap, mert az ár különböző. Alacsonyabb ár → több SOL.

**SELL szintek mennyisége:**
```python
raw_qty = order_quote_value / anchor_price  # NEM a sell ár, hanem az anchor!
qty = round_down_to_step(raw_qty, step_size)
```
Minden SELL szint AZONOS qty-t kap (az anchor áron számolva).

**Miért az anchor?** Mert a sell orderekhez szükséges base-t vagy az anchor közelében szereztük (bootstrap), vagy az elején volt (prebalanced). Az anchor price reprezentálja a "beszerzési árat".

**Validáció minden szintnél:**
```python
result = validate_order(price, qty, symbol_info)
if not result.valid:
    break  # az ezt követő szintek sem lesznek érvényesek
```

Fájl: `src/grid/calculator.py:133-215`

---

## 4. Aritmetikai Grid

### Lépésköz számítás

Az aritmetikai gridnél a szintek **fix összeggel** vannak elosztva:

```
d = grid_step_abs  (abszolút ár különbség, pl. 0.19 USDC)
```

A `d` a profitcélból, díjakból és a **worst case** árból számolódik:

```
d ≥ worst_case_price × ((1 + fee_buy + target_profit / order_quote_value) / (1 - fee_sell) - 1)
```

A `worst_case_price` = anchor_price (a legmagasabb buy ár, ahol a step a legkisebb abszolút értékben).

**Hard floor:** Ugyanaz mint geometriainál — break-even step abszolút padló.

Fájl: `src/grid/calculator.py:70-98`

### Szintek generálása

```
Anchor price: P₀, step: d

BUY szintek:
  Level -1: P₀ - d×1    → pl. 85.46 - 0.19 = 85.27
  Level -2: P₀ - d×2    → pl. 85.46 - 0.38 = 85.08
  Level -3: P₀ - d×3    → pl. 85.46 - 0.57 = 84.89

SELL szintek:
  Level +1: P₀ + d×1    → pl. 85.46 + 0.19 = 85.65
  Level +2: P₀ + d×2    → pl. 85.46 + 0.38 = 85.84
  Level +3: P₀ + d×3    → pl. 85.46 + 0.57 = 86.03
```

**Fontos különbség:** az aritmetikai gridnél a lépésköz (USDC-ben) mindig azonos, de a %-os step az ár függvényében változik:
- Level -1 @ 85.27: step% = 0.19/85.27 = 0.223%
- Level -3 @ 84.89: step% = 0.19/84.89 = 0.224%
- Level +3 @ 86.03: step% = 0.19/86.03 = 0.221%

A geometriainál a %-os step állandó, de az abszolút USDC step változik.

Fájl: `src/grid/calculator.py:217-276`

---

## 5. Grid Count számítás

A `compute_grid_counts()` határozza meg hány BUY és SELL szint lesz:

```python
available = total_capital_quote × (1 - quote_reserve_pct)
k_total = int(available // order_quote_value)
k_total = min(k_total, max_grid_levels)

k_buy = int(k_total × buy_allocation_ratio)
k_sell = k_total - k_buy
```

**Példa:**
```
total_capital_quote = 50 USDC
quote_reserve_pct   = 0.02 (2% tartalék)
order_quote_value   = 5.46 USDC
buy_allocation_ratio = 0.5
max_grid_levels     = 20

available = 50 × 0.98 = 49.0
k_total = int(49.0 / 5.46) = int(8.97) = 8
k_total = min(8, 20) = 8
k_buy = int(8 × 0.5) = 4
k_sell = 8 - 4 = 4
```

**Override lehetőség:** `buy_side_order_count` és `sell_side_order_count` config-ból felülírható, de validálja hogy belefér a tőkébe.

Fájl: `src/grid/calculator.py:100-131`

---

## 6. total_capital_quote és inventory_mode

### Mi az a `total_capital_quote`?

Ez az egyetlen kötelező tőke-paraméter. **A bot által kezelt teljes tőke QUOTE eszközben kifejezve.**

### Hogyan használja a rendszer?

1. **Grid count számítás**: `k_total = available // order_quote_value`
2. **order_quote_value auto-kalkuláció** (ha nem explicit): `order_quote_value = available / max_grid_levels`

**Ez a szám NEM az egyenleged, hanem egy konfigurációs limit** — mennyi tőkét SZÁN a bot a gridre.

### inventory_mode = "prebalanced"

**Feltételezés:** a felhasználó ELŐRE kézileg elosztotta a tőkét base és quote között.

**A flow:**
```
1. Grid count számítás total_capital_quote-ból
   → k_buy=4, k_sell=4 (pl. 50 USDC, ratio=0.5)

2. Account lekérdezés az exchange-ről
   → Ténylegesen van: 25 USDC free + 0.30 SOL free

3. BUY order-ek kirakása:
   FOR level in buy_levels:
       IF inventory.has_quote_for_buy(level.notional):  ← ELLENŐRZI!
           submit_order(level)
       ELSE:
           log.warning("Nincs elég quote")
           continue
   
   → BUY -1 @ 85.27: notional=5.457 → 25 USDC van, OK ✓
   → BUY -2 @ 85.08: notional=5.469 → maradék ~19.5, OK ✓
   → BUY -3 @ 84.89: notional=5.481 → maradék ~14.0, OK ✓
   → BUY -4 @ 84.71: notional=5.493 → maradék ~8.5, OK ✓

4. SELL order-ek kirakása:
   FOR level in sell_levels:
       IF inventory.has_base_for_sell(level.quantity):   ← ELLENŐRZI!
           submit_order(level)
       ELSE:
           log.warning("Nincs elég base")
           break    ← MEGÁLL, mert ha erre nincs, a többire sem lesz
   
   → SELL +1 @ 85.65: qty=0.063 → 0.30 SOL van, OK ✓
   → SELL +2 @ 85.84: qty=0.063 → maradék ~0.237, OK ✓
   → SELL +3 @ 86.03: qty=0.063 → maradék ~0.174, OK ✓
   → SELL +4 @ 86.22: qty=0.063 → maradék ~0.111, OK ✓
```

**A probléma:** a rendszer `total_capital_quote`-ból számolja a grid MÉRETÉT (hány szint), de NEM ellenőrzi előre, hogy van-e elég BASE a sell oldaihoz. Csak kirakáskor derül ki — szintenként.

**Mi kellene igazából?**
A BUY oldalhoz kell: `k_buy × order_quote_value` USDC
A SELL oldalhoz kell: `k_sell × sell_qty` SOL (ahol sell_qty = order_quote_value / anchor_price)

**Prebalanced módban tehát:**
- `total_capital_quote = 50` jelenti: "50 USDC értékű tőkével számolj gridet"
- A rendszer ebből 4+4 szintet számol
- BUY oldalnak kell: ~22 USDC (a free USDC-ből)
- SELL oldalnak kell: ~0.252 SOL (a free SOL-ból)
- Ha nincs elég SOL → kevesebb sell szint lesz (a grid aszimmetrikus)

### inventory_mode = "quote_only_bootstrap"

Minden tőke USDC-ben van. A bot:
1. Grid count számítás → k_buy=4, k_sell=4
2. BUY orderek kirakása → 4 BUY szint
3. Bootstrap: MARKET BUY `k_sell × sell_qty` SOL-ért → ~0.252 SOL
4. Sell orderek kirakása a szerzett SOL-lal

### inventory_mode = "use_existing_balances"

Mint prebalanced, de nincs előzetes elosztás-elvárás. Ami van, azt használja.

Fájl: `src/grid/engine.py:306-333`, `src/grid/inventory.py`

---

## 7. Teljes példa: SOLUSDC geometriai grid

### Kiinduló konfig

```yaml
bot:
  symbol: "SOLUSDC"
  base_asset: "SOL"
  quote_asset: "USDC"
  total_capital_quote: "50"
  order_quote_value: "5.46"
  target_net_profit_per_cycle_quote: "0"     # break-even → legsűrűbb grid
  grid_type: "geometric"
  inventory_mode: "prebalanced"
  buy_allocation_ratio: "0.5"
  quote_reserve_pct: "0.02"
  max_grid_levels: 20
  min_grid_step_pct: "0.001"

fees:
  fee_mode: "maker_assumed"
  maker_fee_buy: "0.001"
  maker_fee_sell: "0.001"
```

### Exchange Filters (SOLUSDC)

```
PRICE_FILTER:  tick_size = 0.01
LOT_SIZE:      step_size = 0.001, min_qty = 0.001
NOTIONAL:      min_notional = 5.00
```

### 1. lépés: Grid step számítás

```
fee_buy = 0.001
fee_sell = 0.001
target_profit = 0  (break-even mód)

break_even_r = (1 + 0.001) / (1 - 0.001) = 1.001 / 0.999 = 1.002002
break_even_step = 1.002002 - 1 = 0.002002 (0.2002%)

target_profit = 0 → r = break_even_r = 1.002002
g = r - 1 = 0.002002

effective_min = max(min_step_pct=0.001, break_even_step=0.002002) = 0.002002
g = max(0.002002, 0.002002) = 0.002002 (0.2002%)

Ellenőrzés: g=0.002002 ≤ max_step_pct=0.05 ✓
```

**Eredmény: grid_step_pct = 0.2002%**

### 2. lépés: Grid count számítás

```
available = 50 × (1 - 0.02) = 49.0
k_total = int(49.0 / 5.46) = 8
k_buy = int(8 × 0.5) = 4
k_sell = 8 - 4 = 4
```

### 3. lépés: Anchor price

```
Anchor = live market mid price = 85.46
```

### 4. lépés: Szintek generálása

```
r = 1 + 0.002002 = 1.002002
```

**BUY szintek:**

| Level | Raw price | Kerekített | Raw qty | Kerekített qty | Notional |
|---|---|---|---|---|---|
| -1 | 85.46 / 1.002002 = 85.289... | 85.29 | 5.46 / 85.29 = 0.06402... | 0.064 | 5.458 |
| -2 | 85.46 / 1.002002² = 85.119... | 85.12 | 5.46 / 85.12 = 0.06415... | 0.064 | 5.447 |
| -3 | 85.46 / 1.002002³ = 84.949... | 84.95 | 5.46 / 84.95 = 0.06427... | 0.064 | 5.436 |
| -4 | 85.46 / 1.002002⁴ = 84.779... | 84.78 | 5.46 / 84.78 = 0.06440... | 0.064 | 5.425 |

Minden BUY szint validáció:
- price tick_size-ra kerekítve ✓
- qty step_size-ra kerekítve ✓
- notional ≥ min_notional (5.0) ✓

**SELL szintek:**

| Level | Raw price | Kerekített | Raw qty | Kerekített qty | Notional |
|---|---|---|---|---|---|
| +1 | 85.46 × 1.002002 = 85.631... | 85.63 | 5.46 / 85.46 = 0.0638... | 0.063 | 5.394 |
| +2 | 85.46 × 1.002002² = 85.802... | 85.80 | 5.46 / 85.46 = 0.0638... | 0.063 | 5.405 |
| +3 | 85.46 × 1.002002³ = 85.974... | 85.97 | 5.46 / 85.46 = 0.0638... | 0.063 | 5.416 |
| +4 | 85.46 × 1.002002⁴ = 86.146... | 86.15 | 5.46 / 85.46 = 0.0638... | 0.063 | 5.427 |

SELL qty mindig `order_quote_value / anchor_price` — anchor-on számolva, nem a sell áron!

### 5. lépés: Profitabilitás ellenőrzés

Egy BUY-SELL ciklus (level -1 → level 0/anchor):
```
BUY @ 85.29: költség = 0.064 × 85.29 × (1 + 0.001) = 5.464
SELL @ 85.46: bevétel = 0.064 × 85.46 × (1 - 0.001) = 5.464

Nettó profit ≈ 0.000 USDC (break-even)
```

Ez helyes! A `target_net_profit_per_cycle_quote: "0"` beállítással a rendszer a lehető legsűrűbb, de NULLSZALDÓS gridet épít. A valódi profit abból jön, hogy:
- Sok ciklus fut le (sűrű grid = több fill)
- A spread és a maker/taker dinamika néha kedvez

### 6. lépés: Tőkeigény

```
BUY oldal: 4 × ~5.45 USDC = ~21.8 USDC
SELL oldal: 4 × 0.063 SOL = 0.252 SOL ≈ 21.5 USDC

Összesen: ~43.3 USDC értékű tőke kell
Konfigurált total: 50 USDC → bőven elfér ✓
Tartalék: 50 - 43.3 = 6.7 USDC puffer
```

### 7. lépés: Grid map

A grid map O(1) lookup tábla counter orderekhez:

```
Level -4: BUY  @ 84.78  qty=0.064
Level -3: BUY  @ 84.95  qty=0.064
Level -2: BUY  @ 85.12  qty=0.064
Level -1: BUY  @ 85.29  qty=0.064
Level  0: ANCHOR @ 85.46  (kerekítve tick-re)
Level +1: SELL @ 85.63  qty=0.063
Level +2: SELL @ 85.80  qty=0.063
Level +3: SELL @ 85.97  qty=0.063
Level +4: SELL @ 86.15  qty=0.063
```

### 8. lépés: Counter order flow

Amikor BUY -1 @ 85.29 FILL-elődik:
```
counter_index = -1 + 1 = 0
counter_price = grid_map[0].price = 85.46  (anchor, kerekítve)
counter_side = SELL
qty = grid_map[-1].quantity = 0.064  (az eredeti BUY level mennyisége)

→ SELL counter @ 85.46, qty=0.064
```

Amikor az a SELL @ 85.46 FILL-elődik:
```
counter_index = 0 - 1 = -1
counter_price = grid_map[-1].price = 85.29
counter_side = BUY
qty = grid_map[0].quantity = 0 → fallback: order_quote_value / 85.29 = 0.064

→ BUY counter @ 85.29, qty=0.064
```

**A ciklus ismétlődik: BUY 85.29 → SELL 85.46 → BUY 85.29 → ...**

---

## Appendix: Aritmetikai grid példa (ugyanazokkal a paraméterekkel)

```
grid_type: "arithmetic"
worst_case_price = anchor_price = 85.46

break_even_r = 1.002002
break_even_step = 0.002002
numerator = 1 + 0.001 + 0/5.46 = 1.001
d = 85.46 × (1.001 / 0.999 - 1) = 85.46 × 0.002002 = 0.1711
d_pct = 0.1711 / 85.46 = 0.002002
effective_min = max(0.001, 0.002002) = 0.002002
d = 85.46 × 0.002002 = 0.1711

Grid step abszolút: 0.17 USDC (kerekítve tick_size-ra)
```

**Szintek:**
```
Level -1: 85.46 - 0.17 = 85.29
Level -2: 85.46 - 0.34 = 85.12
Level -3: 85.46 - 0.51 = 84.95
Level -4: 85.46 - 0.68 = 84.78

Level +1: 85.46 + 0.17 = 85.63
Level +2: 85.46 + 0.34 = 85.80
Level +3: 85.46 + 0.51 = 85.97
Level +4: 85.46 + 0.68 = 86.14
```

Az aritmetikai grid ebben az esetben szinte azonos szinteket ad, mint a geometriai, mert a step kicsi (~0.2%) és az ártartomány szűk.

A különbség nagyobb tőkénél/több szintnél válik jelentőssé:
- **Geometriai:** a szintek logaritmikusan távolodnak → 10% eséshez is van szint
- **Aritmetikai:** lineárisan távolodnak → nagy ármozgásnál gyorsabban elfogy a lefedettség
