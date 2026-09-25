-- Load the CSVs produced by src/etl/etl.py into the schema of
-- Database/postgresql/schema.sql. Run from the project root:
--   psql -d <db> -f Database/postgresql/schema.sql
--   psql -d <db> -f src/etl/load_postgres.sql
-- Order follows the FK dependencies.

\set ON_ERROR_STOP on
BEGIN;
\copy customer                FROM 'data/processed/customer.csv'                WITH (FORMAT csv, HEADER true)
\copy product_group           FROM 'data/processed/product_group.csv'           WITH (FORMAT csv, HEADER true)
\copy product_type            FROM 'data/processed/product_type.csv'            WITH (FORMAT csv, HEADER true)
\copy product                 FROM 'data/processed/product.csv'                 WITH (FORMAT csv, HEADER true)
\copy graphical_appearance    FROM 'data/processed/graphical_appearance.csv'    WITH (FORMAT csv, HEADER true)
\copy colour_group            FROM 'data/processed/colour_group.csv'            WITH (FORMAT csv, HEADER true)
\copy perceived_colour_value  FROM 'data/processed/perceived_colour_value.csv'  WITH (FORMAT csv, HEADER true)
\copy perceived_colour_master FROM 'data/processed/perceived_colour_master.csv' WITH (FORMAT csv, HEADER true)
\copy department              FROM 'data/processed/department.csv'              WITH (FORMAT csv, HEADER true)
\copy garment_group           FROM 'data/processed/garment_group.csv'           WITH (FORMAT csv, HEADER true)
\copy index_group             FROM 'data/processed/index_group.csv'             WITH (FORMAT csv, HEADER true)
\copy article_index           FROM 'data/processed/article_index.csv'           WITH (FORMAT csv, HEADER true)
\copy section                 FROM 'data/processed/section.csv'                 WITH (FORMAT csv, HEADER true)
\copy article                 FROM 'data/processed/article.csv'                 WITH (FORMAT csv, HEADER true)
\copy transaction             FROM 'data/processed/transaction.csv'             WITH (FORMAT csv, HEADER true)
\copy contains                FROM 'data/processed/contains.csv'                WITH (FORMAT csv, HEADER true)
COMMIT;
