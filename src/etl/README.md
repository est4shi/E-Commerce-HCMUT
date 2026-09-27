# ETL (Phase 3)

`etl.py` reads the three Kaggle CSVs with pandas, profiles, cleans and
standardizes them, transforms them according to `Data_Dictionary.docx`, and
validates the result against `Database/postgresql/schema.sql`.

It only produces files. `schema.sql` creates the (empty) tables; the ETL
produces the rows that go into them. Loading into PostgreSQL, indexes,
BigQuery, Power BI and tests are other tasks of `Plan.xlsx` and are not done
here.

## Run

Requires Python 3.10+ and `pip install -r requirements.txt` (pandas, numpy).
Put the raw files in `data/raw/`, then from the project root:

```
python src/etl/etl.py
```

It takes a few minutes and needs a lot of RAM for the 32M transaction rows
(tested on a 32 GB machine).

| Option | Default | |
|---|---|---|
| `--out-dir` | `data/processed` | where the table CSVs and report are written |
| `--price-window-days` | `90` | window for `article.price_base` (Ghi chú B) |

## Input and output

- **Input:** `data/raw/articles.csv`, `customers.csv`, `transactions_train.csv`.
  They are only read, never modified.
- **Output:** `data/processed/<table>.csv`, one per table in `schema.sql`
  (except `login_account`, which is Phase 2 of the Data Dictionary).
  - Each file has a header row and the same column order as the schema.
  - Empty cells mean NULL. Prices are written as plain decimals (10 places).
  - Files are listed parent tables first, so they can be loaded in that order.
- **`data/processed/etl_report.md`:** what each step found and changed, plus
  the PASS/FAIL result of every validation check.

Tables are written only if every check passes. They are written to a temporary
folder first and moved into place at the end, so a crash never leaves a
half-written file. If a check fails, the previous run's tables are removed and
only the report is left.

The pipeline is repeatable: the same raw files always give the same output,
including `transaction_id` (UUID v4 format, generated from a fixed seed).

## Steps

1. **Read CSV:** all article/customer columns as text, so leading zeros survive
   (`article_id` "0108775015").
2. **Profiling:**
   - Row counts, nulls, duplicate keys, value sets, and date and price ranges.
   - Code → name conflicts in `articles.csv`.
   - How many transaction rows get merged into one `contains` line, and the
     largest merged line (flagged if it has more than 100 rows).
3. **Cleaning:** whitespace is trimmed first (ids included), so the key checks
   compare clean values. Then:
   - Articles with a missing id or a missing/non-integer code are dropped, and
     so are duplicate keys.
   - `age` that is not a whole number or is outside 0..120 becomes NULL.
   - Transaction rows are dropped if they have a missing field, an invalid
     channel, a negative price, or an unknown customer/article.
4. **Standardization:**
   - Code columns become integers.
   - `FN`/`Active`: `1.0` → true, empty → false. Other values become NULL and
     are counted.
   - `fashion_news_frequency`: `"None"` → `"NONE"`.
5. **Transformation (Data Dictionary mapping):** produces the rows for every
   table.
   - The 11 lookup tables get the distinct code/name pairs from `articles.csv`.
   - `transaction`: one row per (customer_id, t_dat, sales_channel_id) (Ghi chú A).
   - `contains.quantity`: the number of rows of the same article in the same transaction.
   - `article.image_path`: `images/<first 3 chars>/<article_id>.jpg`.
   - `article.price_base`: 90-day median price, falling back to the all-time
     median; NULL if the article never sold (Ghi chú B).
6. **Validation:**
   - Columns, PRIMARY KEY, NOT NULL, UNIQUE, FOREIGN KEY, CHECK constraints
     and text lengths, checked against `schema.sql`.
   - `sum(contains.quantity)` must equal the number of cleaned transaction rows.

If `schema.sql` changes, update `SCHEMA` / `CHECKS` in `etl.py` to match.

## Decisions and known limitations

- **Product type per product:** `schema.sql` stores one `product_type_no` per
  `product_code` (in `product`). In `articles.csv`, 315 products have articles
  of different types. The ETL keeps the most frequent type (and name), so a few
  articles are counted under another type than their own. Last run: 501
  articles, 137 of them under another product group, 0.59% of revenue. This
  affects "Revenue by Product Type / Group" slightly. The exact numbers are in
  `etl_report.md` on every run.
- **FN / Active:** empty means "no" (false). The source only fills these flags
  when they are set.
- **Price of merged lines:** when a customer bought the same article several
  times on the same day at different prices, `contains.price` is the average
  (team decision). `price * quantity` still equals the real revenue.
- **price_base window:** `t_dat >= last date - 90 days`, exactly as in the Data
  Dictionary, so the window spans 91 dates.
- **image_path:** built from `article_id` only, as the Data Dictionary defines
  it. The ETL does not check whether the image file exists. The Kaggle image
  set is missing some images, so the application has to handle a missing file.
