# Grid Pair Finder

Standalone elemző script — Binance USDC párok rangsorolása grid bot szempontból.

**Bot-tól független.** Nincs Docker, nincs DB, nincs auth. Csak public REST API.

## Mit csinál?

1. Lekéri az összes Binance USDC párt + filtereit (min_notional, tick, step)
2. Likviditás előszűrés (min 50k USDC napi volume)
3. Per-symbol 30 napos napi (1d) OHLCV-t fetcheli
4. Számol leíró metrikákat:
   - **realized_vol_daily** = `std(log_returns_1d)` — napi volatilitás
   - **ATR%** (14d) = `mean(true_range_14) / mean_close × 100`
   - **range_pct_30d** = `(max - min) / mean × 100` az elmúlt 30 napra
   - **trend_strength** = `R²` lineáris regresszió close árakra (0=range, 1=trend)
   - **swing_count** = irány-változások száma
   - **volume_24h_quote** = Binance 24h ticker
5. A `config.yaml` botparamétereivel szimulálja:
   - **V** = `max(min_notional × 1.1, capital / max_grid_levels)` — egy szint értéke
   - **bootstrap_per_sell** = `V × (1 + buffer) / (1 - fee_buy)` — egy sell szint indítási tőkeigénye
   - **K** = `floor(available / (V + bootstrap_per_sell))` — szimmetrikus szintszám
   - **step%** = `(1 + target) / ((1-fee_b)(1-fee_s)) - 1`
   - **profit/cycle** = `V × target_profit_pct`
   - **est cycles/d** = `ATR% / step%` (heuristic — nem backteszt!)
   - **est profit/d** = `profit/cycle × cycles/d × K`
   - **days → 100** = `100 / (profit/d)`
6. Kombinált score → rangsor:
   ```
   score = est_profit_per_day × (1 - 0.5 × trend) × min(volume/100k, 5) × K_factor
   K_factor = 1 ha K≥3, különben 0.3
   ```
7. **HTML report** Plotly grafikonokkal + glosszárium + konkrét számítási példa
8. **CSV export** minden adatra

**NEM backteszt** — csak leíró stat. OHLCV-ből nem lehet pontosan grid működést szimulálni.

## Telepítés (venv-vel — VPS-en is működik)

A rendszer python-jának piszkolása nélkül, izolált virtualenv-ben:

```bash
cd tools/grid_finder
./setup.sh            # létrehoz .venv-t és telepít minden függőséget
```

A `setup.sh` automatikusan a legmagasabb elérhető Python verziót használja
(3.12 → 3.11 → 3.10 → 3.9 → 3 fallback). Python **3.8+** kompatibilis.

Ha nincs `python3-venv` csomag a VPS-en:
```bash
sudo apt install python3-venv python3-pip
```

## Használat

```bash
cp config.example.yaml config.yaml
# szerkeszd a config.yaml-t (capital, target_profit_pct, fee, stb.)

# Első futtatás (Binance REST fetch):
./run.sh config.yaml

# Offline újra-szimuláció (NEM fetcheli újra, csak a meglévő CSV-ből):
./run.sh config.yaml --from-csv
```

A `run.sh` aktiválja a `.venv`-et és a finder.py-t futtatja. Ha valami miatt
közvetlenül akarod hívni:

```bash
source .venv/bin/activate
python finder.py config.yaml
```

Az `--from-csv` mód hasznos ha **csak a config-ot változtatod** (capital, target_pct, fee, buffer)
és gyorsan akarod látni az új eredményt — nem kell várni a teljes Binance fetch-re (másodperces).

Output:
- `output/report.html` — interaktív Plotly riport (glosszárium + számítási példa + 4 grafikon)
- `output/report.md` — **markdown riport** (VPS-en böngésző nélkül is olvasható: `less output/report.md`)
- `output/data.csv` — raw eredmények (Excel/scripting-hez)
- `output/configs/<symbol>.yaml` — **kész bot config** a top 5 párra. Elinditasához:
  ```bash
  cp tools/grid_finder/output/configs/dogsusdc.yaml configs/dogsusdc.yaml
  # majd add hozzá a docker-compose.yml-be a bot-dogsusdc service-t (a generated YAML kommentben)
  docker-compose up -d bot-dogsusdc
  ```

## HTML report tartalma

1. **Top N tábla** — minden metrika színes oszlopokkal. **MinNot** színkód:
   - 🟢 zöld (≤1 USDC) — sűrűbb grid lehetséges
   - 🟡 sárga (≤5 USDC) — standard
   - 🔴 piros (>5 USDC) — drága per-szint, kevés szint fér
2. **📖 Oszlopok magyarázata** — összecsukható glosszárium minden oszlopra a képlettel
3. **🧮 Konkrét számítási példa** — a #1 párra lépésről-lépésre az összes számolás
4. **📈 Vol vs Trend scatter** — magas vol + low trend = ideális
5. **📈 Top 20 bar** — becsült profit/nap vizuálisan
6. **🔥 Sensitivity heatmap** — top 15 pár × 6 különböző target_profit_pct érték

## Korlátok

- A "cycle/nap" becslés egy **közelítés**: feltételezi hogy a napi átlagos ATR-mozgás
  hányszor megy keresztül a step-en. Valós piacon függ:
  - mozgás jellege (squeeze vs trend vs sideways)
  - order book likviditása
  - slippage, fee
- A "K szint párhuzamosan termel" egy **optimista** feltételezés
- A score nem garantál profitot — csak relatív rangsort ad a párok között
- Múltbeli adatok ≠ jövőbeli teljesítmény
