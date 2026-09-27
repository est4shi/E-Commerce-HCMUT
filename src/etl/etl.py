"""Phase 3 ETL for the H&M Personalized Fashion Recommendations dataset.

Scope (Plan.xlsx, Phase 3): read the raw CSVs with pandas, then profiling,
cleaning, standardization, transformation according to the mapping in
Data_Dictionary.docx, and validation against Database/postgresql/schema.sql.
Loading into PostgreSQL, indexing, BigQuery, Power BI and testing belong to
other tasks and are not done here.

Input : data/raw/{articles,customers,transactions_train}.csv (only read, never modified)
Output: data/processed/<table>.csv, one file per schema table with the schema's
        column order, and data/processed/etl_report.md (what every step found).
        Tables are only written when every validation check passes; if a check
        fails, the previous run's tables are removed so they cannot be used by mistake.

The pipeline is repeatable: the same input always gives the same output,
including the generated transaction_ids.

Usage:
    python src/etl/etl.py [--out-dir DIR] [--price-window-days 90]
"""

import argparse
import logging
import os
import shutil
import uuid
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT / "data" / "raw"
RAW_FILES = ("articles.csv", "customers.csv", "transactions_train.csv")

# Fixed seed so transaction_ids are identical on every run (repeatable pipeline).
UUID_SEED = 3127

# Profiling flags contains lines built from more source rows than this, for a manual look.
MANY_REPEATS = 100

log = logging.getLogger("etl")

# articles.csv code -> description columns, one lookup table each (Data Dictionary 7, 9-17).
LOOKUPS = {
    "product_type": ("product_type_no", ["product_type_name", "product_group_name"]),
    "graphical_appearance": ("graphical_appearance_no", ["graphical_appearance_name"]),
    "colour_group": ("colour_group_code", ["colour_group_name"]),
    "perceived_colour_value": ("perceived_colour_value_id", ["perceived_colour_value_name"]),
    "perceived_colour_master": ("perceived_colour_master_id", ["perceived_colour_master_name"]),
    "department": ("department_no", ["department_name"]),
    "garment_group": ("garment_group_no", ["garment_group_name"]),
    "index_group": ("index_group_no", ["index_group_name"]),
    "article_index": ("index_code", ["index_name", "index_group_no"]),
    "section": ("section_no", ["section_name"]),
}

ARTICLE_INT_COLS = [
    "product_code", "product_type_no", "graphical_appearance_no", "colour_group_code",
    "perceived_colour_value_id", "perceived_colour_master_id", "department_no",
    "index_group_no", "section_no", "garment_group_no",
]


class Report:
    """Collects what each step found or changed; saved as etl_report.md."""

    def __init__(self) -> None:
        self.lines: list[str] = []

    def section(self, title: str) -> None:
        self.lines += ["", f"## {title}", ""]
        log.info("== %s", title)

    def add(self, msg: str, *args, level: int = logging.INFO) -> None:
        text = msg % args if args else msg
        self.lines.append(f"- {text}")
        log.log(level, text)

    def write(self, path: Path) -> None:
        path.write_text("# ETL report\n" + "\n".join(self.lines) + "\n", encoding="utf-8")


# ----------------------------------------------------------------------------
# 1. Read CSV
# ----------------------------------------------------------------------------
def read_raw(report: Report) -> dict[str, pd.DataFrame]:
    report.section("1. Read CSV")
    missing = [f for f in RAW_FILES if not (RAW_DIR / f).exists()]
    if missing:
        raise FileNotFoundError(f"missing in {RAW_DIR}: {missing}")

    # Everything as str so codes keep their leading zeros (article_id "0108775015").
    as_str = dict(dtype=str, keep_default_na=False, na_values=[""])
    raw = {
        "articles": pd.read_csv(RAW_DIR / "articles.csv", **as_str),
        "customers": pd.read_csv(RAW_DIR / "customers.csv", **as_str),
        # ~32M rows: categoricals keep the 64-char ids at a few bytes per row.
        "transactions": pd.read_csv(
            RAW_DIR / "transactions_train.csv",
            dtype={"customer_id": "category", "article_id": "category",
                   "price": "float64", "sales_channel_id": "Int8"},
            parse_dates=["t_dat"], date_format="%Y-%m-%d",
        ),
    }
    for name, df in raw.items():
        report.add("%s: %d rows x %d columns", name, len(df), df.shape[1])
    return raw


# ----------------------------------------------------------------------------
# 2. Profiling
# ----------------------------------------------------------------------------
def profile(raw: dict[str, pd.DataFrame], report: Report) -> None:
    report.section("2. Profiling (raw data)")
    a, c, t = raw["articles"], raw["customers"], raw["transactions"]

    for name, df in raw.items():
        nulls = df.isna().sum()
        report.add("%s nulls: %s", name, nulls[nulls > 0].to_dict() or "none")

    report.add("articles: duplicate article_id = %d", a["article_id"].duplicated().sum())
    report.add("customers: duplicate customer_id = %d", c["customer_id"].duplicated().sum())
    for col in ("FN", "Active", "club_member_status", "fashion_news_frequency"):
        report.add("customers.%s values: %s", col, c[col].value_counts(dropna=False).to_dict())
    age = pd.to_numeric(c["age"], errors="coerce")
    report.add("customers.age: min %s, max %s", age.min(), age.max())

    # Code -> description must be 1:1 for the lookup tables of the mapping.
    for key, cols in [("product_code", ["prod_name", "product_type_no"]), *LOOKUPS.values()]:
        for col in cols:
            n = (a.groupby(key)[col].nunique() > 1).sum()
            report.add("articles: %s with >1 %s = %d", key, col, n,
                       level=logging.WARNING if n else logging.INFO)
    for key, cols in LOOKUPS.values():
        n = (a.groupby(cols[0])[key].nunique() > 1).sum()
        if n:
            report.add("articles: %s shared by >1 %s = %d", cols[0], key, n)

    report.add("transactions: t_dat %s .. %s", t["t_dat"].min(), t["t_dat"].max())
    report.add("transactions: price min %.6f, median %.6f, max %.6f",
               t["price"].min(), t["price"].median(), t["price"].max())
    report.add("transactions: sales_channel_id values %s",
               t["sales_channel_id"].value_counts(dropna=False).to_dict())

    # Rows with the same customer/day/channel/article become one contains line.
    rows_per_line = t.groupby(["customer_id", "t_dat", "sales_channel_id", "article_id"], observed=True).size()
    report.add("transactions: %d rows repeat the customer/day/channel/article of another row and are merged "
               "into contains lines (quantity > 1); %d of them are exact duplicates, the rest differ in price",
               rows_per_line.sum() - len(rows_per_line), t.duplicated().sum())
    _, top_day, _, top_article = rows_per_line.idxmax()
    report.add("transactions: largest line = %d rows (article %s on %s); lines with more than %d rows = %d",
               rows_per_line.max(), top_article, str(top_day)[:10], MANY_REPEATS,
               (rows_per_line > MANY_REPEATS).sum(),
               level=logging.WARNING if rows_per_line.max() > MANY_REPEATS else logging.INFO)


# ----------------------------------------------------------------------------
# 3. Cleaning
# ----------------------------------------------------------------------------
def _strip_text(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Trim whitespace in every text column; whitespace-only values become missing."""
    df = df.copy()
    changed = 0
    for col in df.columns:
        s = df[col].str.strip()
        s = s.mask(s == "")
        changed += int((df[col].notna() & (s != df[col])).sum())
        df[col] = s
    return df, changed


def _strip_category(s: pd.Series) -> tuple[pd.Series, int]:
    """Trim whitespace in a categorical column (only the categories are touched)."""
    cats = s.cat.categories
    stripped = cats.str.strip()
    changed = np.flatnonzero(stripped != cats)
    if len(changed) == 0:
        return s, 0
    n = int(s.cat.codes.isin(changed).sum())
    if stripped.is_unique:
        return s.cat.rename_categories(stripped), n
    return s.astype("string").str.strip().astype("category"), n


def clean(raw: dict[str, pd.DataFrame], report: Report):
    report.section("3. Cleaning")
    # Whitespace is trimmed first, so the duplicate and cross-file key checks compare clean ids.

    a, n = _strip_text(raw["articles"])
    report.add("articles: trimmed whitespace in %d values", n)
    codes = a[ARTICLE_INT_COLS].apply(pd.to_numeric, errors="coerce")
    bad = a["article_id"].isna() | codes.isna().any(axis=1) | (codes % 1 != 0).any(axis=1)
    report.add("articles: missing article_id or missing/non-integer code = %d rows dropped", bad.sum())
    a = a.loc[~bad]
    n_before = len(a)
    a = a.drop_duplicates("article_id")
    report.add("articles: dropped %d duplicate article_id rows", n_before - len(a))

    c, n = _strip_text(raw["customers"])
    report.add("customers: trimmed whitespace in %d values", n)
    missing = c["customer_id"].isna()
    report.add("customers: missing customer_id = %d rows dropped", missing.sum())
    c = c.loc[~missing]
    n_before = len(c)
    c = c.drop_duplicates("customer_id").copy()
    report.add("customers: dropped %d duplicate customer_id rows", n_before - len(c))
    age = pd.to_numeric(c["age"], errors="coerce")
    not_whole = c["age"].notna() & (age.isna() | (age % 1 != 0))
    out_of_range = ~not_whole & age.notna() & ~age.between(0, 120)
    c["age"] = age.mask(not_whole | out_of_range)
    report.add("customers: age not a whole number -> NULL = %d", not_whole.sum())
    report.add("customers: age outside 0..120 -> NULL = %d", out_of_range.sum())

    t = raw["transactions"]
    customer_id, n_c = _strip_category(t["customer_id"])
    article_id, n_a = _strip_category(t["article_id"])
    report.add("transactions: trimmed whitespace in %d ids", n_c + n_a)
    t = t.assign(customer_id=customer_id, article_id=article_id,
                 t_dat=pd.to_datetime(t["t_dat"], errors="coerce", format="%Y-%m-%d"))
    rules = {
        "missing customer_id/article_id/t_dat/price": (
            t["customer_id"].isna() | t["article_id"].isna() | t["t_dat"].isna() | t["price"].isna()),
        "sales_channel_id not in (1, 2)": ~t["sales_channel_id"].isin([1, 2]),
        "price < 0": t["price"] < 0,
        "customer_id not in customers.csv": ~t["customer_id"].isin(c["customer_id"]),
        "article_id not in articles.csv": ~t["article_id"].isin(a["article_id"]),
    }
    drop = pd.Series(False, index=t.index)
    for rule, mask in rules.items():
        mask = mask.fillna(True)
        report.add("transactions: %s = %d rows", rule, mask.sum())
        drop |= mask
    t = t.loc[~drop]
    report.add("transactions: dropped %d rows, %d remain", drop.sum(), len(t))
    return a, c, t


# ----------------------------------------------------------------------------
# 4. Standardization
# ----------------------------------------------------------------------------
def _flag(s: pd.Series, name: str, report: Report) -> pd.Series:
    """The source fills FN/Active with 1.0 when set and leaves them empty otherwise:
    1.0 -> true, empty -> false. Anything else is unexpected and becomes NULL."""
    known = s.map({"1.0": True, "1": True, "0.0": False, "0": False})
    unexpected = s.notna() & known.isna()
    out = known.mask(s.isna(), False).astype("boolean")
    report.add("customers.%s: %d true, %d false, %d unexpected values -> NULL", name,
               out.sum(), (~out).sum(), unexpected.sum(),
               level=logging.WARNING if unexpected.any() else logging.INFO)
    return out


def standardize(a: pd.DataFrame, c: pd.DataFrame, report: Report):
    report.section("4. Standardization")

    a = a.copy()
    for col in ARTICLE_INT_COLS:
        a[col] = pd.to_numeric(a[col]).astype("int64")
    report.add("articles: %d code columns converted to integers; article_id kept as 10-char text",
               len(ARTICLE_INT_COLS))

    news = c["fashion_news_frequency"]
    customer = pd.DataFrame({
        "customer_id": c["customer_id"],
        "age": c["age"].astype("Int16"),
        "fn": _flag(c["FN"], "FN", report),
        "active": _flag(c["Active"], "Active", report),
        "club_member_status": c["club_member_status"],
        "fashion_news_frequency": news.replace({"None": "NONE"}),
        "postal_code": c["postal_code"],
    })
    report.add("customers.fashion_news_frequency: 'None' -> 'NONE' = %d", (news == "None").sum())
    return a, customer


# ----------------------------------------------------------------------------
# 5. Transformation (mapping of Data_Dictionary.docx)
# ----------------------------------------------------------------------------
def _lookup(a: pd.DataFrame, key: str, cols: list[str]) -> pd.DataFrame:
    """Distinct (key, cols...) rows; one row per key."""
    t = a[[key, *cols]].drop_duplicates()
    dup = t[key].duplicated(keep=False)
    if dup.any():
        raise ValueError(f"{key} maps to several {cols}: {t.loc[dup, key].unique()[:5]}")
    return t.sort_values(key).reset_index(drop=True)


def _mode_per_key(a: pd.DataFrame, key: str, col: str) -> pd.Series:
    """Most frequent value of col per key (ties -> first seen in the file)."""
    counts = a.groupby([key, col], sort=False).size().rename("n").reset_index()
    counts = counts.sort_values("n", ascending=False, kind="stable")
    return counts.drop_duplicates(key).set_index(key)[col]


def _repeatable_uuid4(n: int) -> np.ndarray:
    """UUID v4-format ids from a fixed seed: the same ids on every run."""
    b = np.random.default_rng(UUID_SEED).integers(0, 256, size=(n, 16), dtype=np.uint8)
    b[:, 6] = (b[:, 6] & 0x0F) | 0x40  # version 4
    b[:, 8] = (b[:, 8] & 0x3F) | 0x80  # RFC 4122 variant
    return np.array([str(uuid.UUID(bytes=row.tobytes())) for row in b], dtype=object)


def transform_articles(a: pd.DataFrame, report: Report) -> dict[str, pd.DataFrame]:
    tables = {name: _lookup(a, key, cols) for name, (key, cols) in LOOKUPS.items()}
    tables["product_group"] = (
        a[["product_group_name"]].drop_duplicates().sort_values("product_group_name").reset_index(drop=True)
    )

    # PRODUCT: some product_codes carry several prod_name / product_type_no
    # across their articles (e.g. "Strap top" vs "Strap top (1)"); keep the most frequent.
    product = pd.concat(
        [_mode_per_key(a, "product_code", "prod_name"),
         _mode_per_key(a, "product_code", "product_type_no")],
        axis=1,
    ).rename_axis("product_code").reset_index()
    tables["product"] = product.sort_values("product_code").reset_index(drop=True)
    report.add("product: %d rows (most frequent prod_name / product_type_no per product_code)", len(product))

    art = a[[
        "article_id", "detail_desc", "product_code", "graphical_appearance_no",
        "colour_group_code", "perceived_colour_value_id", "perceived_colour_master_id",
        "department_no", "garment_group_no", "index_code", "section_no",
    ]].copy()
    # Kaggle image layout: images/<first 3 chars of the 10-char id>/<id>.jpg. Derived from
    # article_id only, as the Data Dictionary defines it; whether the image file exists is
    # not checked here (the Kaggle set lacks some images), the application handles that.
    art.insert(2, "image_path", "images/" + art["article_id"].str[:3] + "/" + art["article_id"] + ".jpg")
    art.insert(3, "price_base", np.nan)
    tables["article"] = art.reset_index(drop=True)
    return tables


def report_product_type_impact(a: pd.DataFrame, tables: dict[str, pd.DataFrame], t: pd.DataFrame,
                               report: Report) -> None:
    """Known limitation: schema.sql keeps one product_type_no per product_code (in PRODUCT),
    but articles.csv sometimes gives articles of the same product different types. Measure
    how many articles, and how much revenue, end up under another type than their own."""
    own = a.set_index("article_id")["product_type_no"]
    kept = a.set_index("article_id")["product_code"].map(
        tables["product"].set_index("product_code")["product_type_no"])
    moved = own.index[own != kept]
    group = tables["product_type"].set_index("product_type_no")["product_group_name"]
    moved_group = int((own[moved].map(group) != kept[moved].map(group)).sum())
    share = t.loc[t["article_id"].isin(moved), "price"].sum() / t["price"].sum()
    report.add("product: %d articles are counted under a different product_type than in articles.csv "
               "(%d of them under a different product_group), %.2f%% of revenue",
               len(moved), moved_group, 100 * share,
               level=logging.WARNING if len(moved) else logging.INFO)


def compute_price_base(t: pd.DataFrame, window_days: int, report: Report) -> pd.Series:
    """Ghi chú B: median price over the last `window_days` days of the dataset,
    falling back to the all-time median for articles not sold in that window.
    `>= max - window_days` follows the Data Dictionary, so the window spans window_days + 1 dates."""
    cutoff = t["t_dat"].max() - pd.Timedelta(days=window_days)
    recent = t.loc[t["t_dat"] >= cutoff].groupby("article_id", observed=True)["price"].median()
    all_time = t.groupby("article_id", observed=True)["price"].median()
    report.add("price_base: %d articles from last %d days, %d from all-time median",
               len(recent), window_days, len(all_time) - len(recent))
    return recent.combine_first(all_time).rename("price_base")


def transform_transactions(t: pd.DataFrame, report: Report) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Ghi chú A: one historical transaction = one (customer_id, t_dat, sales_channel_id).
    tx_idx = t.groupby(["customer_id", "t_dat", "sales_channel_id"], observed=True, sort=False).ngroup().to_numpy()
    n_tx = int(tx_idx.max()) + 1
    uuids = _repeatable_uuid4(n_tx)

    first = pd.Series(np.arange(len(t))).groupby(tx_idx).first().to_numpy()
    transaction = pd.DataFrame({
        "transaction_id": uuids,
        "customer_id": t["customer_id"].to_numpy()[first],
        "sales_channel_id": t["sales_channel_id"].to_numpy()[first].astype("int8"),
        "t_dat": pd.DatetimeIndex(t["t_dat"].to_numpy()[first]).strftime("%Y-%m-%d"),
    })

    # CONTAINS: quantity = number of rows of the same article in the same transaction.
    # If those rows were sold at different prices, the line price is their average
    # (team decision), so price * quantity still equals the revenue of the source rows.
    lines = pd.DataFrame({"tx": tx_idx, "article_id": t["article_id"].to_numpy(), "price": t["price"].to_numpy()})
    agg = lines.groupby(["tx", "article_id"], observed=True, sort=False)["price"].agg(["mean", "size", "nunique"])
    agg = agg.reset_index()
    report.add("contains: %d lines had differing prices across their rows -> averaged",
               (agg["nunique"] > 1).sum())

    contains = pd.DataFrame({
        "transaction_id": uuids[agg["tx"].to_numpy()],
        "article_id": agg["article_id"].astype(str).to_numpy(),
        "price": agg["mean"].round(10).to_numpy(),
        "quantity": agg["size"].to_numpy(),
    })
    report.add("transaction: %d rows, contains: %d rows", len(transaction), len(contains))
    return transaction, contains


def transform(a, customer, t, window_days: int, report: Report) -> dict[str, pd.DataFrame]:
    report.section("5. Transformation (Data Dictionary mapping)")
    tables = transform_articles(a, report)
    tables["customer"] = customer
    report_product_type_impact(a, tables, t, report)

    article = tables["article"]
    article["price_base"] = article["article_id"].map(compute_price_base(t, window_days, report)).round(10)
    report.add("article: %d rows, %d never sold -> price_base NULL", len(article), article["price_base"].isna().sum())

    tables["transaction"], tables["contains"] = transform_transactions(t, report)
    return tables


# ----------------------------------------------------------------------------
# 6. Validation (contract with Database/postgresql/schema.sql)
# ----------------------------------------------------------------------------
# Keep in sync with schema.sql. cols: column order; pk: primary key; nn: NOT NULL;
# uq: UNIQUE; fk: {column: (table, column)}; len: max VARCHAR/CHAR length.
def _simple_lookup(key: str, name: str) -> dict:
    return dict(cols=[key, name], pk=[key], nn=[name], uq=[name], len={name: 50})


SCHEMA = {
    "customer": dict(
        cols=["customer_id", "age", "fn", "active", "club_member_status", "fashion_news_frequency", "postal_code"],
        pk=["customer_id"],
        len={"customer_id": 64, "club_member_status": 20, "fashion_news_frequency": 20, "postal_code": 64}),
    "product_group": dict(cols=["product_group_name"], pk=["product_group_name"], len={"product_group_name": 50}),
    "product_type": dict(
        cols=["product_type_no", "product_type_name", "product_group_name"], pk=["product_type_no"],
        nn=["product_type_name", "product_group_name"],
        fk={"product_group_name": ("product_group", "product_group_name")},
        len={"product_type_name": 50, "product_group_name": 50}),
    "product": dict(
        cols=["product_code", "prod_name", "product_type_no"], pk=["product_code"],
        nn=["prod_name", "product_type_no"], fk={"product_type_no": ("product_type", "product_type_no")},
        len={"prod_name": 100}),
    "graphical_appearance": _simple_lookup("graphical_appearance_no", "graphical_appearance_name"),
    "colour_group": _simple_lookup("colour_group_code", "colour_group_name"),
    "perceived_colour_value": _simple_lookup("perceived_colour_value_id", "perceived_colour_value_name"),
    "perceived_colour_master": _simple_lookup("perceived_colour_master_id", "perceived_colour_master_name"),
    "department": dict(
        cols=["department_no", "department_name"], pk=["department_no"], nn=["department_name"],
        len={"department_name": 50}),
    "garment_group": _simple_lookup("garment_group_no", "garment_group_name"),
    "index_group": _simple_lookup("index_group_no", "index_group_name"),
    "article_index": dict(
        cols=["index_code", "index_name", "index_group_no"], pk=["index_code"],
        nn=["index_name", "index_group_no"], fk={"index_group_no": ("index_group", "index_group_no")},
        len={"index_code": 1, "index_name": 50}),
    "section": dict(cols=["section_no", "section_name"], pk=["section_no"], nn=["section_name"],
                    len={"section_name": 50}),
    "article": dict(
        cols=["article_id", "detail_desc", "image_path", "price_base", "product_code", "graphical_appearance_no",
              "colour_group_code", "perceived_colour_value_id", "perceived_colour_master_id", "department_no",
              "garment_group_no", "index_code", "section_no"],
        pk=["article_id"],
        nn=["product_code", "graphical_appearance_no", "colour_group_code", "perceived_colour_value_id",
            "perceived_colour_master_id", "department_no", "garment_group_no", "index_code", "section_no"],
        fk={"product_code": ("product", "product_code"),
            "graphical_appearance_no": ("graphical_appearance", "graphical_appearance_no"),
            "colour_group_code": ("colour_group", "colour_group_code"),
            "perceived_colour_value_id": ("perceived_colour_value", "perceived_colour_value_id"),
            "perceived_colour_master_id": ("perceived_colour_master", "perceived_colour_master_id"),
            "department_no": ("department", "department_no"),
            "garment_group_no": ("garment_group", "garment_group_no"),
            "index_code": ("article_index", "index_code"),
            "section_no": ("section", "section_no")},
        len={"article_id": 10, "image_path": 100}),
    "transaction": dict(
        cols=["transaction_id", "customer_id", "sales_channel_id", "t_dat"], pk=["transaction_id"],
        nn=["customer_id", "sales_channel_id", "t_dat"], fk={"customer_id": ("customer", "customer_id")}),
    "contains": dict(
        cols=["transaction_id", "article_id", "price", "quantity"], pk=["transaction_id", "article_id"],
        nn=["price", "quantity"],
        fk={"transaction_id": ("transaction", "transaction_id"), "article_id": ("article", "article_id")}),
}

# CHECK constraints and column-type limits: (table, description, rule -> mask of valid rows)
CHECKS = [
    ("customer", "age BETWEEN 0 AND 120", lambda d: d["age"].isna() | d["age"].between(0, 120)),
    ("article", "price_base fits DECIMAL(12,10)", lambda d: d["price_base"].isna() | (d["price_base"].abs() < 100)),
    ("transaction", "sales_channel_id IN (1, 2)", lambda d: d["sales_channel_id"].isin([1, 2])),
    ("contains", "price >= 0", lambda d: d["price"] >= 0),
    ("contains", "price fits DECIMAL(12,10)", lambda d: d["price"] < 100),
    ("contains", "quantity > 0 and fits SMALLINT", lambda d: d["quantity"].between(1, 32767)),
]


def validate(tables: dict[str, pd.DataFrame], n_source_rows: int, report: Report) -> list[str]:
    report.section("6. Validation")
    failed: list[str] = []

    def check(ok: bool, what: str) -> None:
        report.add("%s  %s", "PASS" if ok else "FAIL", what, level=logging.INFO if ok else logging.ERROR)
        if not ok:
            failed.append(what)

    for name, spec in SCHEMA.items():
        df = tables[name]
        check(list(df.columns) == spec["cols"], f"{name}: columns match schema")
        pk = spec["pk"]
        check(df[pk].notna().all().all() and not df.duplicated(pk).any(), f"{name}: PRIMARY KEY {pk}")
        for col in spec.get("nn", []):
            check(df[col].notna().all(), f"{name}.{col} NOT NULL")
        for col in spec.get("uq", []):
            check(not df[col].dropna().duplicated().any(), f"{name}.{col} UNIQUE")
        for col, (ref, ref_col) in spec.get("fk", {}).items():
            check(df[col].dropna().isin(tables[ref][ref_col]).all(), f"{name}.{col} -> {ref}.{ref_col}")
        for col, n in spec.get("len", {}).items():
            check(not (df[col].dropna().astype(str).str.len() > n).any(), f"{name}.{col} length <= {n}")

    for name, what, rule in CHECKS:
        check(bool(rule(tables[name]).all()), f"{name}: CHECK {what}")

    total_qty = int(tables["contains"]["quantity"].sum())
    check(total_qty == n_source_rows,
          f"sum(contains.quantity) = {total_qty} equals cleaned transaction rows = {n_source_rows}")
    return failed


# ----------------------------------------------------------------------------
# Output
# ----------------------------------------------------------------------------
def write_tables(tables: dict[str, pd.DataFrame], out_dir: Path, report: Report) -> None:
    """Write every table into a temporary folder and move them into place only when all
    are written, so a crash never leaves a half-written or mixed set of tables."""
    report.section("Output")
    tmp = out_dir / ".tmp"
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(parents=True)
    for name in SCHEMA:  # parents before children
        # Plain decimals (no 1.69e-05), matching DECIMAL(12,10).
        tables[name].to_csv(tmp / f"{name}.csv", index=False, chunksize=1_000_000, float_format="%.10f")
        report.add("wrote %s.csv (%d rows)", name, len(tables[name]))
    for name in SCHEMA:
        os.replace(tmp / f"{name}.csv", out_dir / f"{name}.csv")
    tmp.rmdir()


def remove_tables(out_dir: Path, report: Report) -> None:
    """After a failed validation, remove the previous run's tables so they cannot be used by mistake."""
    report.section("Output")
    removed = 0
    for name in SCHEMA:
        path = out_dir / f"{name}.csv"
        if path.exists():
            path.unlink()
            removed += 1
    report.add("validation failed: no tables written, %d tables of the previous run removed", removed,
               level=logging.ERROR)


def run(out_dir: Path, window_days: int) -> dict[str, pd.DataFrame]:
    if out_dir.resolve() == RAW_DIR.resolve():
        raise ValueError("--out-dir must not be data/raw: raw files are read-only")
    report = Report()

    raw = read_raw(report)
    profile(raw, report)
    a, c, t = clean(raw, report)
    del raw
    a, customer = standardize(a, c, report)
    tables = transform(a, customer, t, window_days, report)
    failed = validate(tables, len(t), report)

    out_dir.mkdir(parents=True, exist_ok=True)
    if failed:
        remove_tables(out_dir, report)
        report.write(out_dir / "etl_report.md")
        raise SystemExit(f"validation failed ({len(failed)} checks); no tables written, see etl_report.md")
    write_tables(tables, out_dir, report)
    report.write(out_dir / "etl_report.md")
    return tables


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out-dir", type=Path, default=ROOT / "data" / "processed")
    p.add_argument("--price-window-days", type=int, default=90)
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
    run(args.out_dir, args.price_window_days)


if __name__ == "__main__":
    main()
