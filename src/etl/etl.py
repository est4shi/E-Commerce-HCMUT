"""ETL for the H&M Personalized Fashion Recommendations dataset.

Reads articles.csv, customers.csv and transactions_train.csv from data/raw/, transforms them
according to Data_Dictionary.docx, and writes one CSV per table of
Database/postgresql/schema.sql into data/processed/ (ready for \\copy, see
src/etl/load_postgres.sql).

Usage:
    python src/etl/etl.py [--out-dir DIR] [--price-window-days 90]
"""

import argparse
import logging
import time
import uuid
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
log = logging.getLogger("etl")

RAW_FILES = ("articles.csv", "customers.csv", "transactions_train.csv")


# ----------------------------------------------------------------------------
# Extract
# ----------------------------------------------------------------------------
RAW_DIR = ROOT / "data" / "raw"


def check_raw_files() -> None:
    """The raw Kaggle CSVs are read only from data/raw/."""
    missing = [f for f in RAW_FILES if not (RAW_DIR / f).exists()]
    if missing:
        raise FileNotFoundError(f"missing in {RAW_DIR}: {missing}")


def extract_articles(path: Path) -> pd.DataFrame:
    # Everything as str so codes keep their leading zeros (article_id "0108775015").
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])


def extract_customers(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])


def extract_transactions(path: Path) -> pd.DataFrame:
    # ~32M rows: categoricals keep the 64-char customer ids at ~4 bytes/row.
    return pd.read_csv(
        path,
        dtype={
            "customer_id": "category",
            "article_id": "category",
            "price": "float64",
            "sales_channel_id": "int8",
        },
        parse_dates=["t_dat"],
    )


# ----------------------------------------------------------------------------
# Transform — CUSTOMER
# ----------------------------------------------------------------------------
def transform_customer(c: pd.DataFrame) -> pd.DataFrame:
    flag = {"1.0": True, "1": True, "0.0": False, "0": False}
    out = pd.DataFrame({
        "customer_id": c["customer_id"],
        "age": pd.to_numeric(c["age"]).astype("Int16"),
        # Source only holds 1.0 or empty; empty is kept as NULL.
        "fn": c["FN"].map(flag).astype("boolean"),
        "active": c["Active"].map(flag).astype("boolean"),
        "club_member_status": c["club_member_status"],
        # Dataset mixes "NONE" and "None" for the same value.
        "fashion_news_frequency": c["fashion_news_frequency"].replace({"None": "NONE"}),
        "postal_code": c["postal_code"],
    })
    out = out.drop_duplicates("customer_id")
    out.loc[~out["age"].between(0, 120), "age"] = pd.NA
    return out


# ----------------------------------------------------------------------------
# Transform — article hierarchy / lookups
# ----------------------------------------------------------------------------
def _lookup(a: pd.DataFrame, key: str, cols: list[str]) -> pd.DataFrame:
    """Distinct (key, cols...) rows; one row per key (key -> cols is verified 1:1)."""
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


def transform_article_tables(a: pd.DataFrame) -> dict[str, pd.DataFrame]:
    int_cols = [
        "product_code", "product_type_no", "graphical_appearance_no", "colour_group_code",
        "perceived_colour_value_id", "perceived_colour_master_id", "department_no",
        "index_group_no", "section_no", "garment_group_no",
    ]
    a = a.copy()
    a[int_cols] = a[int_cols].astype("int64")

    tables = {
        "product_group": (
            a[["product_group_name"]].drop_duplicates()
            .sort_values("product_group_name").reset_index(drop=True)
        ),
        "product_type": _lookup(a, "product_type_no", ["product_type_name", "product_group_name"]),
        "graphical_appearance": _lookup(a, "graphical_appearance_no", ["graphical_appearance_name"]),
        "colour_group": _lookup(a, "colour_group_code", ["colour_group_name"]),
        "perceived_colour_value": _lookup(a, "perceived_colour_value_id", ["perceived_colour_value_name"]),
        "perceived_colour_master": _lookup(a, "perceived_colour_master_id", ["perceived_colour_master_name"]),
        "department": _lookup(a, "department_no", ["department_name"]),
        "garment_group": _lookup(a, "garment_group_no", ["garment_group_name"]),
        "index_group": _lookup(a, "index_group_no", ["index_group_name"]),
        "article_index": _lookup(a, "index_code", ["index_name", "index_group_no"]),
        "section": _lookup(a, "section_no", ["section_name"]),
    }

    # PRODUCT: in the source some product_codes carry several prod_name /
    # product_type_no values across their articles (e.g. "Strap top" vs
    # "Strap top (1)"); PRODUCT keeps the most frequent one.
    for col in ("prod_name", "product_type_no"):
        n_bad = (a.groupby("product_code")[col].nunique() > 1).sum()
        if n_bad:
            log.warning("product: %d product_codes have >1 %s -> keeping most frequent", n_bad, col)
    product = pd.concat(
        [_mode_per_key(a, "product_code", "prod_name"),
         _mode_per_key(a, "product_code", "product_type_no")],
        axis=1,
    ).rename_axis("product_code").reset_index()
    tables["product"] = product.sort_values("product_code").reset_index(drop=True)

    # ARTICLE (price_base is filled in later from transactions).
    art = a[[
        "article_id", "detail_desc", "product_code", "graphical_appearance_no",
        "colour_group_code", "perceived_colour_value_id", "perceived_colour_master_id",
        "department_no", "garment_group_no", "index_code", "section_no",
    ]].copy()
    # Kaggle image layout: images/<first 3 chars of the 10-char id>/<id>.jpg
    art.insert(2, "image_path", "images/" + art["article_id"].str[:3] + "/" + art["article_id"] + ".jpg")
    art.insert(3, "price_base", np.nan)
    tables["article"] = art.drop_duplicates("article_id").reset_index(drop=True)
    return tables


# ----------------------------------------------------------------------------
# Transform — TRANSACTION / CONTAINS / price_base
# ----------------------------------------------------------------------------
def compute_price_base(t: pd.DataFrame, window_days: int) -> pd.Series:
    """Ghi chú B: median price over the last `window_days` days of the dataset,
    falling back to the all-time median for articles not sold in that window."""
    cutoff = t["t_dat"].max() - pd.Timedelta(days=window_days)
    recent = t.loc[t["t_dat"] >= cutoff].groupby("article_id", observed=True)["price"].median()
    all_time = t.groupby("article_id", observed=True)["price"].median()
    log.info("price_base: %d articles from last %d days, %d from all-time fallback",
             len(recent), window_days, len(all_time) - len(recent))
    return recent.combine_first(all_time).rename("price_base")


def transform_transactions(
    t: pd.DataFrame, customer_ids: pd.Index, article_ids: pd.Index
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Referential integrity: drop rows whose customer / article is unknown.
    ok = t["customer_id"].isin(customer_ids) & t["article_id"].isin(article_ids)
    ok &= t["sales_channel_id"].isin([1, 2]) & (t["price"] >= 0) & t["t_dat"].notna()
    if (~ok).any():
        log.warning("transactions: dropping %d rows failing FK/domain checks", (~ok).sum())
        t = t.loc[ok]

    # Ghi chú A: one historical transaction = one (customer_id, t_dat, sales_channel_id).
    tx_keys = ["customer_id", "t_dat", "sales_channel_id"]
    tx_idx = t.groupby(tx_keys, observed=True, sort=False).ngroup().to_numpy()
    n_tx = int(tx_idx.max()) + 1
    uuids = np.array([str(uuid.uuid4()) for _ in range(n_tx)], dtype=object)

    first = pd.Series(np.arange(len(t))).groupby(tx_idx).first().to_numpy()
    transaction = pd.DataFrame({
        "transaction_id": uuids,
        "customer_id": t["customer_id"].to_numpy()[first],
        "sales_channel_id": t["sales_channel_id"].to_numpy()[first],
        "t_dat": t["t_dat"].to_numpy()[first],
    })
    transaction["t_dat"] = transaction["t_dat"].dt.strftime("%Y-%m-%d")

    # CONTAINS: quantity = number of duplicate rows of the same article in the
    # same transaction. If those duplicates were sold at different prices, the
    # line price is their average.
    lines = pd.DataFrame({"tx": tx_idx, "article_id": t["article_id"].to_numpy(), "price": t["price"].to_numpy()})
    g = lines.groupby(["tx", "article_id"], observed=True, sort=False)["price"]
    contains = g.agg(["mean", "size", "nunique"]).reset_index()
    n_mixed = int((contains["nunique"] > 1).sum())
    if n_mixed:
        log.info("contains: %d lines had differing prices across duplicates -> averaged", n_mixed)
    if contains["size"].max() > np.iinfo(np.int16).max:
        raise ValueError("quantity exceeds SMALLINT")

    contains = pd.DataFrame({
        "transaction_id": uuids[contains["tx"].to_numpy()],
        "article_id": contains["article_id"].astype(str).to_numpy(),
        "price": contains["mean"].round(10).to_numpy(),
        "quantity": contains["size"].astype("int16").to_numpy(),
    })
    return transaction, contains


# ----------------------------------------------------------------------------
# Load
# ----------------------------------------------------------------------------
# Order matters for FK constraints when loading into PostgreSQL.
LOAD_ORDER = [
    "customer", "product_group", "product_type", "product", "graphical_appearance",
    "colour_group", "perceived_colour_value", "perceived_colour_master", "department",
    "garment_group", "index_group", "article_index", "section", "article",
    "transaction", "contains",
]


def load(tables: dict[str, pd.DataFrame], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for name in LOAD_ORDER:
        df = tables[name]
        path = out_dir / f"{name}.csv"
        started = time.perf_counter()
        df.to_csv(path, index=False, chunksize=1_000_000)
        log.info("wrote %-24s %11d rows  (%.1fs)", path.name, len(df), time.perf_counter() - started)


# ----------------------------------------------------------------------------
def run(out_dir: Path, window_days: int) -> dict[str, pd.DataFrame]:
    check_raw_files()
    raw = RAW_DIR
    log.info("reading raw data from %s", raw)

    customer = transform_customer(extract_customers(raw / "customers.csv"))
    log.info("customer: %d rows", len(customer))

    tables = transform_article_tables(extract_articles(raw / "articles.csv"))
    tables["customer"] = customer
    log.info("article: %d rows, product: %d rows", len(tables["article"]), len(tables["product"]))

    started = time.perf_counter()
    t = extract_transactions(raw / "transactions_train.csv")
    log.info("transactions_train: %d rows read (%.1fs)", len(t), time.perf_counter() - started)

    article = tables["article"]
    price_base = compute_price_base(t, window_days)
    article["price_base"] = article["article_id"].map(price_base).round(10)
    log.info("article: %d without any sale -> price_base NULL", article["price_base"].isna().sum())

    tables["transaction"], tables["contains"] = transform_transactions(
        t, pd.Index(customer["customer_id"]), pd.Index(article["article_id"])
    )
    del t
    log.info("transaction: %d rows, contains: %d rows (quantity total %d)",
             len(tables["transaction"]), len(tables["contains"]), tables["contains"]["quantity"].sum())

    load(tables, out_dir)
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
