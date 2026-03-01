import re
import yaml
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CFG_PATH = ROOT / "schema" / "config.yml"
OUT_DIR = ROOT / "initdb" / "staging"
OUT_FILE = OUT_DIR / "iceberg_staging.sql"

WAREHOUSE_BASE = "s3://staging/warehouse"

def load_cfg() -> dict:
    with open(CFG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def norm_type(t: str) -> str:
    return re.sub(r"\s+", " ", t.strip().lower())

def pg_to_trino_type(pg_type: str) -> str:
    t = norm_type(pg_type)
    if t in ("serial", "bigserial", "smallserial"):
        return "bigint"
    if t in ("int", "integer", "int4"):
        return "integer"
    if t in ("bigint", "int8"):
        return "bigint"
    if t in ("smallint", "int2"):
        return "smallint"
    if t in ("decimal", "numeric"):
        return "decimal(38,10)"
    m = re.match(r"^(decimal|numeric)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)\s*$", t)
    if m:
        return f"decimal({m.group(2)},{m.group(3)})"
    if t in ("varchar", "character varying", "text", "citext"):
        return "varchar"
    m = re.match(r"^(varchar|character varying)\s*\(\s*(\d+)\s*\)\s*$", t)
    if m:
        return f"varchar({m.group(2)})"
    if t in ("bool", "boolean"):
        return "boolean"
    if t == "date":
        return "date"
    if t.startswith("timestamp"):
        m = re.match(r"^timestamp\s*\(\s*(\d+)\s*\)\s*$", t)
        return f"timestamp({m.group(1)})" if m else "timestamp(3)"
    if t == "uuid":
        return "uuid"
    if t in ("inet", "cidr"):
        return "varchar(64)"
    if t in ("json", "jsonb"):
        return "json"
    if t in ("double precision", "float8"):
        return "double"
    if t in ("real", "float4"):
        return "real"
    return "varchar"

def create_schema_sql(schema: str) -> str:
    location = f"{WAREHOUSE_BASE}/{schema}"
    return f"CREATE SCHEMA IF NOT EXISTS iceberg.{schema} WITH (location = '{location}');\n"

def create_table_sql(schema: str, table_name: str, columns: dict, tech_cols: dict) -> str:
    col_lines = []
    for col_name, meta in columns.items():
        src_type = meta if isinstance(meta, str) else meta.get("type", "varchar")
        col_lines.append(f"  {col_name:<30} {pg_to_trino_type(src_type)}")
    for col_name, meta in tech_cols.items():
        src_type = meta if isinstance(meta, str) else meta.get("type", "varchar")
        col_lines.append(f"  {col_name:<30} {pg_to_trino_type(src_type)}")
    cols_block = ",\n".join(col_lines)
    return f"""CREATE TABLE IF NOT EXISTS iceberg.{schema}.{table_name} (
{cols_block}
);
"""

def main():
    cfg = load_cfg()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    tech = cfg.get("tech_cols", {}).get("staging", {})

    out = []
    out.append("-- Auto-generated STAGING layer DDL \n\n")

    for src in cfg["sources"]:
        ns = src["database"]
        out.append(f"-- STAGING layer: {ns}\n")
        out.append(create_schema_sql(ns))
        out.append("\n")
        for t in src["tables"]:
            out.append(f"-- from source table: {ns}.{t['name']}\n")
            out.append(create_table_sql(ns, t["name"], t["columns"], tech))
            out.append("\n")

    OUT_FILE.write_text("".join(out), encoding="utf-8")
    print(f"Wrote {OUT_FILE}")

if __name__ == "__main__":
    main()