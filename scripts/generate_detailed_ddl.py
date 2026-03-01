import yaml
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CFG_PATH = ROOT / "schema" / "config.yml"
OUT_DIR = ROOT / "initdb" / "detailed"

DETAILED_WAREHOUSE_BASE = "s3://detailed/warehouse"

ICEBERG_CATALOG = "iceberg"

def load_cfg():
    cfg_path = CFG_PATH
    if not cfg_path.exists():
        alt = Path(__file__).resolve().parent / "config.yml"
        if alt.exists():
            cfg_path = alt
    with open(cfg_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def ns_from_source_table(src_table: str) -> str:
    return src_table.split(".", 1)[0]

def tbl_from_source_table(src_table: str) -> str:
    return src_table.split(".", 1)[1]

def map_trino_type(src_type: str) -> str:
    t = (src_type or "").strip().lower()
    if t == "serial":
        return "bigint"
    if t in {"int", "integer"}:
        return "integer"
    if t == "bigint":
        return "bigint"
    if t == "uuid":
        return "uuid"
    if t == "boolean":
        return "boolean"
    if t == "date":
        return "date"
    if t == "timestamp":
        return "timestamp(3)"
    if t == "decimal":
        return "decimal(38,10)"
    if t == "inet":
        return "varchar(64)"
    if t == "text":
        return "varchar"
    return t


def create_namespace_block(ns: str) -> str:
    return (
        f"CREATE SCHEMA IF NOT EXISTS {ICEBERG_CATALOG}.{ns} "
        f"WITH (location = '{DETAILED_WAREHOUSE_BASE}/{ns}');\n\n"
    )

def find_table(cfg: dict, source_table: str) -> dict:
    db = ns_from_source_table(source_table)
    tbl = tbl_from_source_table(source_table)
    for s in cfg["sources"]:
        if s["database"] == db:
            for t in s["tables"]:
                if t["name"] == tbl:
                    return t
    raise KeyError(f"Table not found: {source_table}")

def sat_attrs(cfg: dict, source_table: str) -> list[str]:
    t = find_table(cfg, source_table)
    cols = t["columns"]
    out = []
    for c, meta in cols.items():
        if meta.get("pk") is True:
            continue
        if meta.get("bk") is True:
            continue
        if meta.get("scd2") is True:
            continue
        out.append(c)
    return out

def col_type(cfg: dict, source_table: str, col: str) -> str:
    t = find_table(cfg, source_table)
    return map_trino_type(t["columns"][col]["type"])


def hub_bk_type(cfg: dict, hub: dict) -> str:
    bk_cols = hub.get("bk") or []
    if len(bk_cols) != 1:
        return "varchar"
    col = bk_cols[0]
    return col_type(cfg, hub["source_table"], col)

def hub_ddl(ns: str, hub: dict) -> str:
    name = hub["name"]
    bk_cols = hub["bk"]
    bk_name = "bk_" + "_".join(bk_cols)
    bk_type = hub_bk_type(cfg, hub)
    return f"""CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.{ns}.{name} (
  hk            varchar,
  {bk_name}     {bk_type},
  load_dts      timestamp(3),
  record_source varchar
);
"""

def sat_ddl(ns: str, sat: dict, attrs: list[str]) -> str:
    name = sat["name"]
    hub_name = sat["hub"]
    hk_col = "hk_" + hub_name.replace("hub_", "")

    cols_sql = []
    for a in attrs:
        cols_sql.append(f"  {a:<30} {col_type(cfg, sat['source_table'], a)}")
    cols_block = ",\n".join(cols_sql)

    return f"""CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.{ns}.{name} (
  {hk_col}       varchar,
  hashdiff       varchar,
  effective_from timestamp(3),
  effective_to   timestamp(3),
  is_current     boolean,
  load_dts       timestamp(3),
  record_source  varchar{("," if attrs else "")}
{cols_block}
);
"""

def link_ddl(ns: str, link: dict) -> str:
    name = link["name"]
    left = "hk_" + link["left_hub"].replace("hub_", "")
    right = "hk_" + link["right_hub"].replace("hub_", "")
    return f"""CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.{ns}.{name} (
  hk_link       varchar,
  {left}        varchar,
  {right}       varchar,
  load_dts      timestamp(3),
  record_source varchar
);
"""

def hub_map(dv: dict) -> dict:
    return {h["name"]: h for h in dv.get("hubs", [])}

def validate_link(cfg: dict, dv: dict, link: dict):
    hubs = hub_map(dv)

    def must_exist(hub_name: str):
        if hub_name not in hubs:
            raise ValueError(f"Link {link['name']}: unknown hub '{hub_name}'")

    must_exist(link["left_hub"])
    must_exist(link["right_hub"])

    left_hub = hubs[link["left_hub"]]
    right_hub = hubs[link["right_hub"]]

    left_tbl = left_hub["source_table"]
    right_tbl = right_hub["source_table"]

    left_cols = find_table(cfg, left_tbl)["columns"]
    right_cols = find_table(cfg, right_tbl)["columns"]

    for c in link["left_bk"]:
        if c not in left_cols:
            raise ValueError(f"Link {link['name']}: left_bk column '{c}' not in {left_tbl}")
        if left_cols[c].get("bk") is not True:
            raise ValueError(f"Link {link['name']}: left_bk '{c}' must be marked bk:true in {left_tbl}")

    for c in link["right_bk"]:
        if c in right_cols and right_cols[c].get("bk") is True:
            continue

        st = link["source_table"]
        st_cols = find_table(cfg, st)["columns"]
        if c not in st_cols:
            raise ValueError(f"Link {link['name']}: right_bk column '{c}' not in {right_tbl} nor in {st}")

        ref = st_cols[c].get("ref")
        if not ref:
            raise ValueError(f"Link {link['name']}: right_bk '{c}' must be bk:true in {right_tbl} or have ref in {st}")

        ref_db_tbl, ref_col = ref.rsplit(".", 1)
        if ref_col not in right_hub["bk"]:
            raise ValueError(f"Link {link['name']}: right_bk '{c}' ref '{ref}' must point to one of BK {right_hub['bk']}")

def main():
    global cfg
    cfg = load_cfg()
    out_dir = OUT_DIR
    if not out_dir.parent.exists():
        out_dir = Path(__file__).resolve().parent / "initdb" / "detailed"
    out_dir.mkdir(parents=True, exist_ok=True)

    dv = cfg["detailed_dv2"]

    # Валидация links заранее
    for link in dv.get("links", []):
        validate_link(cfg, dv, link)

    # Группируем объекты по namespace (по source_table)
    buckets = {}
    def add(ns: str, kind: str, obj: dict):
        buckets.setdefault(ns, {}).setdefault(kind, []).append(obj)

    for hub in dv.get("hubs", []):
        add(ns_from_source_table(hub["source_table"]), "hubs", hub)
    for sat in dv.get("satellites", []):
        add(ns_from_source_table(sat["source_table"]), "satellites", sat)
    for link in dv.get("links", []):
        add(ns_from_source_table(link["source_table"]), "links", link)

    content = [
        "-- Auto-generated DETAILED layer DDL for Trino + Iceberg\n\n"
    ]

    for ns, objs in buckets.items():
        content.append(f"-- DETAILED layer: {ns}\n")
        content.append(create_namespace_block(ns))

        content.append("-- Hubs\n")
        for hub in objs.get("hubs", []):
            content.append(hub_ddl(ns, hub))
            content.append("\n")

        content.append("-- Satellites (SCD Type2)\n")
        for sat in objs.get("satellites", []):
            attrs = sat_attrs(cfg, sat["source_table"])
            content.append(sat_ddl(ns, sat, attrs))
            content.append("\n")

        content.append("-- Links\n")
        for link in objs.get("links", []):
            content.append(link_ddl(ns, link))
            content.append("\n")

    out_path = out_dir / "iceberg_detailed.sql"
    out_path.write_text("".join(content), encoding="utf-8")
    print(f"Wrote {out_path}")

if __name__ == "__main__":
    main()