import re
import yaml
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CFG_PATH = ROOT / "schema" / "config.yml"
OUT_DIR = ROOT / "initdb" / "source"


def load_cfg():
    with open(CFG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_table_index(cfg: dict) -> dict:
    idx = {}
    for src in cfg["sources"]:
        db = src["database"]
        for t in src["tables"]:
            idx[(db, t["name"])] = t
    return idx


def parse_ref(ref: str) -> tuple[str, str, str]:
    parts = ref.split(".")
    if len(parts) != 3:
        raise ValueError(f"Bad ref format: '{ref}', expected '<db>.<table>.<column>'")
    return parts[0], parts[1], parts[2]


def sanitize_ident(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]+", "_", s)


def pk_cols(columns: dict) -> list[str]:
    return [c for c, m in columns.items() if m.get("pk") is True]


def bk_cols(columns: dict) -> list[str]:
    return [c for c, m in columns.items() if m.get("bk") is True]


def ddl_table(db: str, schema: str, table: dict, table_index: dict) -> str:
    name = table["name"]
    cols = table["columns"]

    pk = pk_cols(cols)
    bk = bk_cols(cols)

    inline_pk_col = None
    if len(pk) == 1:
        c = pk[0]
        if cols[c]["type"].lower() == "serial":
            inline_pk_col = c

    inline_bk_unique_col = None
    if len(bk) == 1:
        c = bk[0]
        if c != inline_pk_col and cols[c].get("pk") is not True:
            inline_bk_unique_col = c

    col_lines = []
    for col_name, meta in cols.items():
        col_def = f"{col_name:<18} {meta['type']}"

        if inline_pk_col == col_name:
            col_def += " PRIMARY KEY"

        if inline_bk_unique_col == col_name:
            col_def += " UNIQUE"

        col_lines.append("  " + col_def)

    constraints = []
    fk_comments = []

    if pk and inline_pk_col is None:
        constraints.append(
            f"CONSTRAINT pk_{sanitize_ident(name)} PRIMARY KEY ({', '.join(pk)})"
        )

    if len(bk) > 1:
        constraints.append(
            f"CONSTRAINT uq_{sanitize_ident(name)} UNIQUE ({', '.join(bk)})"
        )

    for col_name, meta in cols.items():
        ref = meta.get("ref")
        if not ref:
            continue

        ref_db, ref_table, ref_col = parse_ref(ref)

        if ref_db != db:
            continue

        target = table_index.get((ref_db, ref_table))
        if not target:
            raise KeyError(
                f"FK ref target table not found: {ref_db}.{ref_table} "
                f"(from {db}.{name}.{col_name})"
            )
        if ref_col not in target["columns"]:
            raise KeyError(
                f"FK ref target column not found: {ref_db}.{ref_table}.{ref_col} "
                f"(from {db}.{name}.{col_name})"
            )

        c_name = f"fk_{sanitize_ident(name)}__{sanitize_ident(col_name)}"
        constraints.append(
            f"CONSTRAINT {c_name} FOREIGN KEY ({col_name}) REFERENCES {schema}.{ref_table}({ref_col})"
        )

    lines = []
    lines.append(f"CREATE TABLE IF NOT EXISTS {schema}.{name} (\n")
    lines.append(",\n".join(col_lines))
    if constraints:
        lines.append(",\n  " + ",\n  ".join(constraints))
    lines.append("\n);\n")

    if fk_comments:
        lines.append("\n" + "\n".join(fk_comments) + "\n")

    return "".join(lines)


def main():
    cfg = load_cfg()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    table_index = build_table_index(cfg)

    dbs = sorted({s["database"] for s in cfg["sources"]})
    db_file = OUT_DIR / "00-create-databases.sql"
    db_sql = [
        "-- Auto-generated PostgreSQL databases DDL\n\n",
    ] + [f"CREATE DATABASE {db};\n" for db in dbs]
    db_file.write_text("".join(db_sql), encoding="utf-8")
    print(f"Wrote {db_file}")

    for src in cfg["sources"]:
        db = src["database"]
        schema = src.get("schema", "public")

        content = [
            f"\\connect {db};\n",
            f"-- database: {db}\n\n",
        ]

        for t in src["tables"]:
            content.append(ddl_table(db=db, schema=schema, table=t, table_index=table_index))
            content.append("\n")

        out_path = OUT_DIR / f"{db}.sql"
        out_path.write_text("".join(content), encoding="utf-8")
        print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()