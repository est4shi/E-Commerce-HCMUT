# ETL (Phase 3)

`etl.py` builds one CSV per table of `Database/postgresql/schema.sql` from the
Kaggle files, following `Data_Dictionary.docx`. Below are only how to run it
and the choices the guideline files leave open.

## Run

With the raw files in `data/raw/`, from the project root:

```
pip install -r requirements.txt
python src/etl/etl.py
```

Options: `--out-dir` (default `data/processed`) and `--price-window-days`
(default 90, the N of Ghi chú B). Tested with Python 3.11, pandas 2.1.4 and
numpy 1.26.3; it takes a few minutes and needs a lot of RAM (tested with 32 GB).

## Output

`<table>.csv` for every table except `login_account`: header row, schema
column order, empty cell = NULL. Load them in the order `schema.sql` creates
the tables (parents first).

The ETL runs once, so a run simply deletes the tables of any earlier run and
writes new ones only if every check against `schema.sql` passes. Profiling
results and failed checks are printed to the console. Load the tables only
after a run that ended with `done`.

## Decisions

- **Invalid source rows** (missing or duplicate ids, missing fields, unknown
  customer/article, invalid code, channel or price) are dropped, and invalid
  ages become NULL. Anything else that breaks `schema.sql` fails validation.
- **`product`:** some `product_code`s have articles with different
  `prod_name`/`product_type_no`; the most frequent is kept. So 501 articles
  (0.59% of revenue) count under another product type, 137 of them under
  another product group.
- **`FN` / `Active`:** empty means false.
- **`contains.price`:** if the rows merged into one line had different prices,
  it is their average, so `price * quantity` still equals the revenue.
- **`price_base`:** falls back to the all-time median; NULL only if the
  article never sold.
- **`transaction_id`:** drawn from a fixed seed, the only random step, so a
  rerun on the same input and machine gives identical output.
- **`image_path`:** not checked against the image folder; some Kaggle images
  are missing.

If `schema.sql` changes, update `SCHEMA` / `CHECKS` in `etl.py`.
