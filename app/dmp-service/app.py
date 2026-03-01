import requests
import json
import os
import time
import yaml
import datetime as dt
import base64
from decimal import Decimal
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

from kafka import KafkaConsumer
from trino.dbapi import connect
from kafka.errors import NoBrokersAvailable


@dataclass
class TableSpec:
    namespace: str
    table: str
    columns: List[str]
    col_types: Dict[str, str]


def utc_now_timestamp3() -> str:
    return dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def utc_now_timestamp6() -> str:
    return dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")


def load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def parse_debezium_envelope(value: dict) -> Tuple[Optional[dict], Optional[dict], str, Optional[int]]:
    payload = value.get("payload", value)
    return payload.get("before"), payload.get("after"), payload.get("op") or "?", payload.get("ts_ms")


def topic_to_source_table(topic: str) -> Tuple[str, str, str]:
    parts = topic.split(".")
    if len(parts) < 3:
        raise ValueError(f"Unexpected topic name: {topic}")
    return parts[0], parts[1], ".".join(parts[2:])


class DMPService:
    def __init__(self, cfg_path: str):
        self.cfg = load_yaml(cfg_path)
        print("schema loaded successfully")
        self.prefix_map: Dict[str, str] = {
            "user_service": "user_service_db",
            "order_service": "order_service_db",
            "logistics_service": "logistics_service_db",
        }

        self.tech_cols: List[str] = list(self.cfg.get("tech_cols", {}).get("staging", {}).keys()) or [
            "__op", "__ts_ms", "__kafka_topic", "__kafka_partition", "__kafka_offset", "__ingest_dts", "__record_source"
        ]

        self.table_specs: Dict[Tuple[str, str], TableSpec] = {}
        self.tech_types = self.cfg.get("tech_cols", {}).get("staging", {})
        for src in self.cfg.get("sources", []):
            ns = src["database"]
            for t in src.get("tables", []):
                col_names = list(t.get("columns", {}).keys())
                col_types = {name: info.get("type") for name, info in t["columns"].items()}
                self.table_specs[(ns, t["name"])] = TableSpec(ns, t["name"], col_names, col_types)

        print("=== Table specs loaded ===")
        for (ns, tbl), spec in self.table_specs.items():
            print(f"{ns}.{tbl}: columns={spec.columns}")
            print(f"  types={spec.col_types}")

        # Trino
        self.trino_host = os.environ.get("TRINO_HOST", "trino")
        self.trino_port = int(os.environ.get("TRINO_PORT", "8080"))
        self.trino_user = os.environ.get("TRINO_USER", "dmp")
        self.trino_catalog = os.environ.get("TRINO_CATALOG", "iceberg")
        print("[dmp-service] Await Trino connection...")

        session = requests.Session()
        session.timeout = (5, 10)

        try:
            self.conn = connect(
                host=self.trino_host,
                port=self.trino_port,
                user=self.trino_user,
                catalog=self.trino_catalog,
                schema="default",
                http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
                http_session=session,
            )
            print("Trino connected successfully")
        except Exception as e:
            print("Trino connection error:")
            raise

        # Kafka
        bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092")
        group_id = os.environ.get("KAFKA_GROUP_ID", "dmp-service")
        topics = os.environ.get("KAFKA_TOPICS", "").strip()
        self.topics = [t.strip() for t in topics.split(",") if t.strip()]
        if not self.topics:
            raise RuntimeError("Set KAFKA_TOPICS env var (comma-separated debezium topics).")

        bootstrap_servers = [s.strip() for s in bootstrap.split(",") if s.strip()]

        deadline = time.time() + int(os.environ.get("KAFKA_CONNECT_TIMEOUT_SEC", "300"))
        last_err = None

        print("[dmp-service] Await Kafka connection...")
        while time.time() < deadline:
            try:
                self.consumer = KafkaConsumer(
                    *self.topics,
                    bootstrap_servers=bootstrap_servers,
                    group_id=group_id,
                    enable_auto_commit=False,
                    auto_offset_reset=os.environ.get("KAFKA_AUTO_OFFSET_RESET", "earliest"),
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    key_deserializer=lambda m: m.decode("utf-8") if m else None,
                    consumer_timeout_ms=1000,
                    request_timeout_ms=30000,
                    api_version_auto_timeout_ms=30000,
                )
                last_err = None
                print(f"[dmp-service] Consumer is up: {bootstrap_servers}")
                break
            except NoBrokersAvailable as e:
                last_err = e
                print(f"[dmp-service] Kafka not ready at {bootstrap_servers}, retrying...")
                time.sleep(3)

        if last_err:
            raise last_err

        self.batch_size = int(os.environ.get("BATCH_SIZE", "200"))
        self.flush_interval_sec = int(os.environ.get("FLUSH_INTERVAL_SEC", "3"))
        self.buffer: Dict[Tuple[str, str], List[dict]] = {}
        self.last_flush = time.time()

        self.total_inserted = 0
        self.topic_counts = defaultdict(int)
        self.last_log_thousand = 0

    def _resolve_target(self, topic: str) -> TableSpec:
        prefix, _schema, table = topic_to_source_table(topic)
        ns = self.prefix_map.get(prefix)
        if not ns:
            raise KeyError(f"No prefix mapping for '{prefix}' (topic={topic})")
        key = (ns, table)
        if key not in self.table_specs:
            raise KeyError(f"Table not found in schema config: {ns}.{table} (topic={topic})")
        return self.table_specs[key]

    def _norm_type(self, col_type: Optional[str]) -> Optional[str]:
        if not col_type:
            return None
        t = str(col_type).strip().lower()
        if t == "int":
            return "integer"
        if t == "serial":
            return "bigint"
        if t == "timestamp":
            return "timestamp(6)"
        if t in ("text", "citext", "inet"):
            return "varchar"
        if t in ("json", "jsonb"):
            return "json"
        return t

    def _row_from_event(self, spec: TableSpec, topic: str, partition: int, offset: int, event_value: dict) -> dict:
        before, after, op, ts_ms = parse_debezium_envelope(event_value)
        image = after if op in ("c", "u", "r") else before if op == "d" else after or before or {}

        row: Dict[str, Any] = {c: image.get(c) for c in spec.columns}

        row["__op"] = op
        row["__ts_ms"] = ts_ms
        row["__kafka_topic"] = topic
        row["__kafka_partition"] = partition
        row["__kafka_offset"] = offset
        row["__ingest_dts"] = utc_now_timestamp6()
        row["__record_source"] = topic
        return row

    def _lit(self, v: Any, col_type: Optional[str] = None) -> str:
        col_type = self._norm_type(col_type)
        if v is None:
            return f"CAST(NULL AS {col_type})" if col_type else "NULL"

        if col_type == "uuid":
            return f"UUID '{v}'"

        if col_type in ("integer", "bigint", "decimal", "boolean"):
            if isinstance(v, bool):
                return "TRUE" if v else "FALSE"

            if col_type == "decimal" and isinstance(v, dict) and "scale" in v and "value" in v:
                scale = int(v["scale"])
                raw = base64.b64decode(v["value"])
                unscaled = int.from_bytes(raw, byteorder="big", signed=True)
                dec = Decimal(unscaled).scaleb(-scale)
                return format(dec, "f")

            return str(v)

        if col_type in ("timestamp(6)", "date"):
            if isinstance(v, (int, float)):
                x = float(v)
                if x >= 1e18:
                    sec = x / 1e9
                elif x >= 1e15:
                    sec = x / 1e6
                elif x >= 1e12:
                    sec = x / 1e3
                else:
                    sec = x
                dt_obj = dt.datetime.utcfromtimestamp(sec)

                if col_type == "date":
                    return f"DATE '{dt_obj.date().isoformat()}'"

                v_str = dt_obj.strftime("%Y-%m-%d %H:%M:%S.%f")

            elif isinstance(v, dt.datetime):
                if col_type == "date":
                    return f"DATE '{v.date().isoformat()}'"
                v_str = v.strftime("%Y-%m-%d %H:%M:%S.%f")
            elif isinstance(v, dt.date):
                return f"DATE '{v.isoformat()}'" if col_type == "date" else f"CAST(TIMESTAMP '{v.isoformat()} 00:00:00.000000' AS timestamp(6))"

            else:
                v_str = str(v).strip()
                if col_type == "date":
                    v_date = v_str.split("T", 1)[0].split(" ", 1)[0]
                    return f"DATE '{v_date}'"

            if "." in v_str:
                head, frac = v_str.split(".", 1)
                frac = (frac + "000000")[:6]
                v_str = f"{head}.{frac}"
            else:
                v_str = v_str + ".000000"

            return f"CAST(TIMESTAMP '{v_str}' AS timestamp(6))"

        return "'" + str(v).replace("'", "''") + "'"

    def _flush_table(self, spec: TableSpec, rows: List[dict]):
        if not rows:
            return

        first_row = rows[0]
        topic = first_row.get("__kafka_topic", "unknown")

        cols = spec.columns + self.tech_cols
        values_sql = []
        for r in rows:
            row_values = []
            for c in cols:
                val = r.get(c)
                if c in spec.col_types:
                    col_type = spec.col_types[c]
                elif c in self.tech_types:
                    col_type = self.tech_types[c].get('type')
                else:
                    col_type = None
                row_values.append(self._lit(val, col_type))
            values_sql.append("(" + ", ".join(row_values) + ")")

        sql = (
                f"INSERT INTO {self.trino_catalog}.{spec.namespace}.{spec.table} "
                f"({', '.join(cols)}) VALUES \n" + ",\n".join(values_sql)
        )

        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            cur.fetchall()

            self.total_inserted += len(rows)
            self.topic_counts[topic] += len(rows)

            current_thousand = self.total_inserted // 1000
            if current_thousand > self.last_log_thousand:
                self.last_log_thousand = current_thousand
                print(
                    f"[dmp-service] Loaded {self.total_inserted} records total. "
                    f"Per topic: {dict(self.topic_counts)}"
                )

        except Exception as e:
            print(f"Error inserting into {spec.namespace}.{spec.table}: {e}")
            print(f"SQL-query: {sql[:200]}...")
            raise

    def flush(self):
        for (ns, table), rows in list(self.buffer.items()):
            self._flush_table(self.table_specs[(ns, table)], rows)
        self.buffer.clear()
        self.last_flush = time.time()

    def run(self):
        print("dmp-service started. Topics:", self.topics)
        while True:
            got = False
            for msg in self.consumer:
                got = True
                spec = self._resolve_target(msg.topic)
                row = self._row_from_event(spec, msg.topic, msg.partition, msg.offset, msg.value)

                key = (spec.namespace, spec.table)
                self.buffer.setdefault(key, []).append(row)

                if len(self.buffer[key]) >= self.batch_size:
                    self.flush()
                    self.consumer.commit()

            if self.buffer and (time.time() - self.last_flush >= self.flush_interval_sec):
                self.flush()
                self.consumer.commit()

            if not got:
                time.sleep(0.5)


if __name__ == "__main__":
    print("Starting dmp-service")
    cfg_path = os.environ.get("SCHEMA_CONFIG_PATH", "/schema/config.yml")
    print(f"Loading schema from {cfg_path}")
    DMPService(cfg_path).run()