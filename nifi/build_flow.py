"""
Builds the ABC Hub ETL pipeline in a running Apache NiFi 2.x instance through
the REST API, then exports it as a flow definition (ABC_Hub_ETL_Pipeline.json).

    python nifi/build_flow.py                    # build (replaces an existing copy) + export
    python nifi/build_flow.py --export-only      # just re-export the current flow

Building through the API (instead of hand-editing JSON) guarantees the
exported definition is exactly what NiFi itself produces, and makes the flow
reproducible and reviewable as code.

Flow layout (top-level process group "ABC Hub ETL Pipeline"):

  01 Extract ──► 02 Land Raw (Bronze)
      │
      └──────► 03 Validate & Cleanse ──► 04 Load Silver
                        │  rejected / failed         │ failed
                        ▼                            ▼
               06 Error Handling & Audit Logging ◄───┘
                        ▲
  05 Build Gold (Star Schema) ── failed ─┘
"""
import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from nifi_api import NiFi, NiFiError  # noqa: E402
from flow_config import TABLES, REFERENCE, avro_schema, cleanse_sql, duplicates_sql  # noqa: E402

FLOW_NAME = "ABC Hub ETL Pipeline"
PARAM_CONTEXT = "ABC Hub ETL Parameters"
EXPORT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ABC_Hub_ETL_Pipeline.json")

# Production schedules (Quartz cron). Downstream processors are event driven.
CRON_REFERENCE = "0 30 0 * * ?"      # reference data: daily at 00:30
CRON_TRANSACTIONAL = "0 5 * * * ?"   # transactional tables: hourly at :05
CRON_GOLD = "0 30 * * * ?"           # gold build: hourly at :30 (after extracts settle)

PARAMETERS = [
    ("source.jdbc.url", "jdbc:postgresql://localhost:5432/abc_hub_operational", False,
     "JDBC URL of the operational (source) PostgreSQL database"),
    ("target.jdbc.url", "jdbc:postgresql://localhost:5432/abc_hub_analytics", False,
     "JDBC URL of the analytical PostgreSQL database"),
    ("db.user", "postgres", False, "Database user for both pools (use a least-privilege ETL role in production)"),
    ("db.password", None, True,
     "Database password. Leave empty to let the PostgreSQL JDBC driver read the pgpass file "
     "(%APPDATA%\\postgresql\\pgpass.conf on Windows, ~/.pgpass on Linux/macOS)"),
    ("jdbc.driver.path", r"D:\DATA ENGINEERING\tools\postgresql-42.7.13.jar", False,
     "Absolute path to the PostgreSQL JDBC driver jar"),
    ("log.directory", "./logs/abc_hub_etl", False,
     "Folder for rejected-record and failed-FlowFile files (relative paths resolve against NIFI_HOME)"),
    ("gold.full.refresh", "false", False,
     "true = rebuild every gold fact from silver; false = incremental (only changed keys)"),
]

GOLD_STEPS = [
    # (processor name, SQL function, comment)
    ("Prepare Gold Change Set", "etl.gold_prepare",
     "Flags cross-batch duplicates in silver, extends dim_date/dim_month and records which customers, "
     "months and inventory items changed since the last successful run (etl.gold_change_set)."),
    ("Load dim_subscription_plan (SCD1)", "etl.load_dim_subscription_plan", "Upserts plans (Type 1 overwrite)."),
    ("Load dim_content (SCD1)", "etl.load_dim_content",
     "Upserts content with type, streamable/rentable flags, primary genre and artist."),
    ("Load dim_warehouse (SCD1)", "etl.load_dim_warehouse", "Upserts warehouses with city and country."),
    ("Load dim_customer (SCD2)", "etl.load_dim_customer",
     "Type 2 on status and home location; Type 1 on contact details."),
    ("Load dim_inventory_item (SCD2)", "etl.load_dim_inventory_item",
     "Type 2 on status, condition and warehouse."),
    ("Load fact_customer_daily_activity", "etl.load_fact_customer_daily_activity",
     "Scenario A: re-aggregates changed customers (delete + insert in one transaction)."),
    ("Load fact_content_monthly_performance", "etl.load_fact_content_monthly_performance",
     "Scenario B: re-aggregates changed months for all content."),
    ("Load fact_inventory_daily_snapshot", "etl.load_fact_inventory_daily_snapshot",
     "Scenario C: rebuilds changed items and extends every item to the new snapshot end date."),
    ("Finalize Gold Run", "etl.gold_finalize", "Advances the gold watermark and refreshes planner statistics."),
]


def proc(pg_id, pid):
    return {"id": pid, "groupId": pg_id, "type": "PROCESSOR"}


def port(pg_id, pid, kind):
    return {"id": pid, "groupId": pg_id, "type": kind}


# ---------------------------------------------------------------------------
# Tear-down of a previous build (makes the builder re-runnable)
# ---------------------------------------------------------------------------
def remove_existing(n, root):
    for pg in n.get(f"/flow/process-groups/{root}")["processGroupFlow"]["flow"]["processGroups"]:
        if pg["component"]["name"] != FLOW_NAME:
            continue
        pg_id = pg["id"]
        print(f"  removing existing '{FLOW_NAME}' ({pg_id})")
        n.set_group_state(pg_id, "STOPPED")
        time.sleep(2)
        n.put(f"/flow/process-groups/{pg_id}/controller-services", {"id": pg_id, "state": "DISABLED"})
        time.sleep(2)

        def drop_all(g):
            for c in n.get(f"/process-groups/{g}/connections")["connections"]:
                n.post(f"/flowfile-queues/{c['id']}/drop-requests", {})
            for child in n.get(f"/process-groups/{g}/process-groups")["processGroups"]:
                drop_all(child["id"])
        drop_all(pg_id)
        time.sleep(2)
        entity = n.get(f"/process-groups/{pg_id}")
        n.delete(f"/process-groups/{pg_id}?version={entity['revision']['version']}")
    for ctx in n.get("/flow/parameter-contexts")["parameterContexts"]:
        if ctx["component"]["name"] == PARAM_CONTEXT:
            n.delete(f"/parameter-contexts/{ctx['id']}?version={ctx['revision']['version']}")


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------
def build(n):
    root = n.root_id()
    remove_existing(n, root)

    ctx = n.create_parameter_context(
        PARAM_CONTEXT, "Connection, path and run settings for the ABC Hub ETL pipeline",
        [{"name": k, "value": val, "sensitive": s, "description": d} for k, val, s, d in PARAMETERS])
    top = n.create_process_group(
        root, FLOW_NAME, 0, 0, parameter_context_id=ctx["id"],
        comments="Z2D DWDF Mini Project 01 - ABC Hub. Operational PostgreSQL -> bronze/silver/gold "
                 "analytical PostgreSQL. See README.md for how to run.")["id"]

    # ---------------------------------------------------- controller services
    def dbcp(name, url_param, comment):
        return n.create_controller_service(top, "DBCPConnectionPool", name, {
            "Database Connection URL": f"#{{{url_param}}}",
            "Database Driver Class Name": "org.postgresql.Driver",
            "Database Driver Locations": "#{jdbc.driver.path}",
            "Database User": "#{db.user}",
            "Password": "#{db.password}",
            "Max Total Connections": "8",
            "Validation Query": "SELECT 1",
        }, comment)["id"]

    cs = {
        "src": dbcp("Operational DB Pool (source)", "source.jdbc.url", "Read-only access to abc_hub_operational"),
        "tgt": dbcp("Analytics DB Pool (target)", "target.jdbc.url", "Read/write access to abc_hub_analytics"),
        "w_plain": n.create_controller_service(top, "JsonRecordSetWriter", "JSON Writer (plain)", {
            "Schema Write Strategy": "no-schema", "Date Format": "yyyy-MM-dd",
            "Timestamp Format": "yyyy-MM-dd HH:mm:ss"},
            "Writes JSON arrays without schema metadata (extraction and SQL results)")["id"],
        "w_schema": n.create_controller_service(top, "JsonRecordSetWriter", "JSON Writer (sets avro.schema)", {
            "Schema Write Strategy": "full-schema-attribute", "Date Format": "yyyy-MM-dd",
            "Timestamp Format": "yyyy-MM-dd HH:mm:ss"},
            "Writes JSON and records the output schema in the avro.schema attribute for the next reader")["id"],
        "r_schema": n.create_controller_service(top, "JsonTreeReader", "JSON Reader (avro.schema attribute)", {
            "Schema Access Strategy": "schema-text-property", "Schema Text": "${avro.schema}",
            "Date Format": "yyyy-MM-dd", "Timestamp Format": "yyyy-MM-dd HH:mm:ss"},
            "Reads JSON with the typed schema carried in avro.schema")["id"],
        "r_raw": n.create_controller_service(top, "JsonTreeReader", "JSON Reader (raw.schema attribute)", {
            "Schema Access Strategy": "schema-text-property", "Schema Text": "${raw.schema}",
            "Date Format": "yyyy-MM-dd", "Timestamp Format": "yyyy-MM-dd HH:mm:ss"},
            "Reads extracted JSON with the all-nullable raw schema (bronze landing)")["id"],
    }

    # ------------------------------------------------------------ groups
    # Child groups do not inherit the parameter context over the API, so each
    # one is bound to it explicitly.
    def group(name, x, y, comments):
        return n.create_process_group(top, name, x, y, comments, parameter_context_id=ctx["id"])["id"]

    # Layout (top level): extract on the left, bronze + gold on the upper row,
    # validate + silver on the middle row, error handling at the bottom right.
    pgs = {
        "extract": group("01 Extract - Operational DB (incremental)", 0, 0,
            "QueryDatabaseTableRecord per source table; state = max(updated_at) per table (watermark)."),
        "bronze": group("02 Land Raw - Bronze (insert-only history)", 650, -300,
            "Appends every extracted row version to bronze.<table> with batch lineage. Idempotent (INSERT_IGNORE)."),
        "cleanse": group("03 Validate & Cleanse", 650, 250,
            "ValidateRecord (mandatory fields, types) then QueryRecord (standardise, fix, flag, de-duplicate)."),
        "silver": group("04 Load Silver (upsert)", 1300, 250,
            "Upserts cleansed records into silver.<table> by primary key. Idempotent."),
        "gold": group("05 Build Gold - Star Schema", 1300, -300,
            "Scheduled ELT: dimensions first, then facts, via PostgreSQL functions. Incremental + idempotent."),
        "errors": group("06 Error Handling & Audit Logging", 1300, 800,
            "Rejected records -> file + etl.rejected_record; failed FlowFiles -> file + error log."),
    }
    n.create_label(top, "ABC Hub ETL Pipeline  |  operational PostgreSQL  ->  bronze  ->  silver  ->  gold (star schema)\n"
                        "01 -> 02 & 03 -> 04 run event-driven after each extract  |  05 runs on its own schedule  |  "
                        "06 catches everything that is rejected or fails",
                   0, -470, width=1700, height=70, font_size="16px")

    build_extract(n, pgs["extract"], cs)
    build_bronze(n, pgs["bronze"], cs)
    build_cleanse(n, pgs["cleanse"], cs)
    build_silver(n, pgs["silver"], cs)
    build_gold(n, pgs["gold"], cs)
    build_errors(n, pgs["errors"], cs)
    wire_groups(n, top, pgs)

    for cs_id in cs.values():
        n.enable_controller_service(cs_id)
    return top


def funnel(pg_id, fid):
    return {"id": fid, "groupId": pg_id, "type": "FUNNEL"}


# Canvas geometry: a processor is ~352 x 128 px. Rows are spaced so each
# connection label fits between boxes; fan-ins go through funnels so no
# connection line crosses a component.
ROW = 200


def build_extract(n, pg, cs):
    ref = [t for t in TABLES if t["group"] == REFERENCE]
    txn = [t for t in TABLES if t["group"] != REFERENCE]
    blocks = ((ref, 0, CRON_REFERENCE, "REFERENCE DATA - cron '%s' (daily 00:30)"),
              (txn, 1150, CRON_TRANSACTIONAL, "TRANSACTIONAL DATA - cron '%s' (hourly at :05)"))
    out = n.create_port(pg, "OUTPUT_PORT", "Extracted batches", 2150, -60,
                        "Every extracted table batch, tagged with its transformation rules")["id"]
    for tables, x0, cron, title in blocks:
        n.create_label(pg, title % cron, x0, -110, 792, 50)
        mid_y = (len(tables) - 1) * ROW / 2 + 40
        fun = n.create_funnel(pg, x0 + 880, mid_y)["id"]
        for i, t in enumerate(tables):
            y = i * ROW
            q = n.create_processor(pg, "QueryDatabaseTableRecord", f"Extract {t['name']} (incremental)", x0, y, {
                "Database Connection Pooling Service": cs["src"],
                "Database Type": "PostgreSQL",
                "Table Name": t["name"],
                "Record Writer": cs["w_plain"],
                "Maximum-value Columns": "updated_at",
                "Initial Load Strategy": "Start at Beginning",
                "Use Avro Logical Types": "true",
                "Fetch Size": "5000",
                "Max Rows Per FlowFile": "0",
            }, schedule={"strategy": "CRON_DRIVEN", "period": cron},
                comments=f"Incremental extract of public.{t['name']}: only rows with updated_at greater than the "
                         f"stored maximum (processor state). First run = full load.")["id"]
            tag = n.create_processor(pg, "UpdateAttribute", f"Tag {t['name']} batch", x0 + 440, y, {
                "source.table": t["name"],
                "pk.columns": ",".join(t["pk"]),
                "batch.id": "${UUID()}",
                "raw.schema": json.dumps(avro_schema(t, nullable_all=True)),
                "avro.schema": json.dumps(avro_schema(t)),
                # UpdateAttribute evaluates Expression Language in its own values; "$${" keeps
                # ${batch.id} literal here so QueryRecord resolves it per FlowFile later.
                "cleanse.sql": cleanse_sql(t).replace("${", "$${"),
                "duplicates.sql": duplicates_sql(t),
            }, comments="Attaches the table's schemas and cleansing SQL (generated from nifi/flow_config.py) so "
                        "the shared validate/cleanse/load stages stay generic.")["id"]
            n.connect(pg, proc(pg, q), proc(pg, tag), ["success"])
            n.connect(pg, proc(pg, tag), funnel(pg, fun), ["success"])
        # funnel -> output port: straight up the gap right of the block, then along the top edge
        bends = [(x0 + 904, -25), (2150, -25)] if x0 == 0 else None
        n.connect(pg, funnel(pg, fun), port(pg, out, "OUTPUT_PORT"), bends=bends)


def build_bronze(n, pg, cs):
    inp = n.create_port(pg, "INPUT_PORT", "Extracted batches", 55, 0)["id"]
    fail = n.create_port(pg, "OUTPUT_PORT", "Processing failures", 560, 720)["id"]
    fun = n.create_funnel(pg, 600, 470)["id"]
    lineage = n.create_processor(pg, "UpdateRecord", "Add batch lineage field", 0, 180, {
        "Record Reader": cs["r_raw"], "Record Writer": cs["w_schema"],
        "Replacement Value Strategy": "literal-value", "/_batch_id": "${batch.id}",
    }, comments="Adds _batch_id so every bronze row can be traced to the NiFi batch that loaded it.")["id"]
    put = n.create_processor(pg, "PutDatabaseRecord", "Append to bronze.<table>", 0, 430, {
        "Record Reader": cs["r_schema"], "Database Type": "PostgreSQL", "Statement Type": "INSERT_IGNORE",
        "Database Connection Pooling Service": cs["tgt"], "Schema Name": "bronze", "Table Name": "${source.table}",
        "Unmatched Column Behavior": "Ignore Unmatched Columns", "Unmatched Field Behavior": "Fail on Unmatched Fields",
    }, comments="INSERT ... ON CONFLICT (primary key = source pk + updated_at) DO NOTHING: replaying a batch "
                "never duplicates history.",
        retry={"relationships": ["retry"], "count": 3})["id"]
    audit = n.create_processor(pg, "PutSQL", "Audit bronze load", 0, 680, {
        "JDBC Connection Pool": cs["tgt"], "Support Fragmented Transactions": "false",
        "SQL Statement": "INSERT INTO etl.load_audit (batch_id, source_table, target_layer, record_count, flowfile_uuid) "
                         "VALUES ('${batch.id}', '${source.table}', 'bronze', ${record.count:replaceNull(0)}, '${uuid}')",
    }, auto_terminate=["success"], comments="One etl.load_audit row per batch.")["id"]
    n.connect(pg, port(pg, inp, "INPUT_PORT"), proc(pg, lineage))
    n.connect(pg, proc(pg, lineage), proc(pg, put), ["success"])
    n.connect(pg, proc(pg, put), proc(pg, audit), ["success"])
    for p, rels in ((lineage, ["failure"]), (put, ["failure", "retry"]), (audit, ["failure", "retry"])):
        n.connect(pg, proc(pg, p), funnel(pg, fun), rels)
    n.connect(pg, funnel(pg, fun), port(pg, fail, "OUTPUT_PORT"))


def build_cleanse(n, pg, cs):
    inp = n.create_port(pg, "INPUT_PORT", "Extracted batches", 55, 0)["id"]
    out = n.create_port(pg, "OUTPUT_PORT", "Clean batches", 55, 760)["id"]
    rej = n.create_port(pg, "OUTPUT_PORT", "Rejected records", 1150, 330)["id"]
    fail = n.create_port(pg, "OUTPUT_PORT", "Processing failures", -330, 760)["id"]
    fun = n.create_funnel(pg, -250, 360)["id"]
    validate = n.create_processor(pg, "ValidateRecord", "Validate mandatory fields & types", 0, 180, {
        "Record Reader": cs["r_schema"], "Record Writer": cs["w_schema"],
        "Record Writer for Invalid Records": cs["w_schema"], "Schema Access Strategy": "reader-schema",
        "Allow Extra Fields": "false", "Strict Type Checking": "false",
        "Validation Details Attribute Name": "validation.details", "Maximum Validation Details Length": "2000",
    }, comments="Validates each record against the table's avro.schema: mandatory fields must be non-null and every "
                "value must match its type. Invalid records are split off - valid ones continue.",
        concurrency=2)["id"]
    cleanse = n.create_processor(pg, "QueryRecord", "Cleanse, standardise & de-duplicate", 0, 480, {
        "Record Reader": cs["r_schema"], "Record Writer": cs["w_schema"],
        "Include Zero Record FlowFiles": "false",
        "cleansed": "${cleanse.sql}", "duplicates": "${duplicates.sql}",
    }, auto_terminate=["original"],
        comments="Runs the table's cleanse.sql (trim/case, sign fixes, caps, derived values, dq_flags, keep first of "
                 "each duplicate group) and duplicates.sql (the later copies, sent to the reject log).",
        concurrency=2)["id"]
    tag_invalid = n.create_processor(pg, "UpdateAttribute", "Tag validation rejects", 600, 180,
                                     {"reject.reason": "VALIDATION"})["id"]
    tag_dup = n.create_processor(pg, "UpdateAttribute", "Tag duplicate rejects", 600, 480, {
        "reject.reason": "DUPLICATE",
        "validation.details": "In-batch duplicate: same business key as an earlier record, which was kept"})["id"]
    n.connect(pg, port(pg, inp, "INPUT_PORT"), proc(pg, validate))
    n.connect(pg, proc(pg, validate), proc(pg, cleanse), ["valid"])
    n.connect(pg, proc(pg, validate), proc(pg, tag_invalid), ["invalid"])
    n.connect(pg, proc(pg, cleanse), port(pg, out, "OUTPUT_PORT"), ["cleansed"])
    n.connect(pg, proc(pg, cleanse), proc(pg, tag_dup), ["duplicates"])
    n.connect(pg, proc(pg, tag_invalid), port(pg, rej, "OUTPUT_PORT"), ["success"])
    n.connect(pg, proc(pg, tag_dup), port(pg, rej, "OUTPUT_PORT"), ["success"])
    n.connect(pg, proc(pg, validate), funnel(pg, fun), ["failure"])
    n.connect(pg, proc(pg, cleanse), funnel(pg, fun), ["failure"])
    n.connect(pg, funnel(pg, fun), port(pg, fail, "OUTPUT_PORT"))


def build_silver(n, pg, cs):
    inp = n.create_port(pg, "INPUT_PORT", "Clean batches", 55, 0)["id"]
    fail = n.create_port(pg, "OUTPUT_PORT", "Processing failures", 560, 600)["id"]
    fun = n.create_funnel(pg, 600, 350)["id"]
    put = n.create_processor(pg, "PutDatabaseRecord", "Upsert into silver.<table>", 0, 180, {
        "Record Reader": cs["r_schema"], "Database Type": "PostgreSQL", "Statement Type": "UPSERT",
        "Database Connection Pooling Service": cs["tgt"], "Schema Name": "silver", "Table Name": "${source.table}",
        "Update Keys": "${pk.columns}",
        "Unmatched Column Behavior": "Ignore Unmatched Columns", "Unmatched Field Behavior": "Fail on Unmatched Fields",
    }, comments="INSERT ... ON CONFLICT (pk) DO UPDATE: a changed source row replaces its silver version; "
                "replaying a batch is harmless.", retry={"relationships": ["retry"], "count": 3},
        concurrency=2)["id"]
    audit = n.create_processor(pg, "PutSQL", "Audit silver load", 0, 430, {
        "JDBC Connection Pool": cs["tgt"], "Support Fragmented Transactions": "false",
        "SQL Statement": "INSERT INTO etl.load_audit (batch_id, source_table, target_layer, record_count, flowfile_uuid) "
                         "VALUES ('${batch.id}', '${source.table}', 'silver', ${record.count:replaceNull(0)}, '${uuid}')",
    })["id"]
    log = n.create_processor(pg, "LogMessage", "Log silver load", 0, 680, {
        "Log Level": "info", "Log Prefix": "[ABC Hub ETL] ",
        "Log Message": "silver.${source.table}: upserted ${record.count} records (batch ${batch.id})",
    }, auto_terminate=["success"])["id"]
    n.connect(pg, port(pg, inp, "INPUT_PORT"), proc(pg, put))
    n.connect(pg, proc(pg, put), proc(pg, audit), ["success"])
    n.connect(pg, proc(pg, audit), proc(pg, log), ["success"])
    n.connect(pg, proc(pg, put), funnel(pg, fun), ["failure", "retry"])
    n.connect(pg, proc(pg, audit), funnel(pg, fun), ["failure", "retry"])
    n.connect(pg, funnel(pg, fun), port(pg, fail, "OUTPUT_PORT"))


def build_gold(n, pg, cs):
    """U-shaped chain: column A runs down (trigger -> dimensions), column B runs
    back up (facts -> completion). Failures of each column go through a funnel
    on its outer side to 'Mark Gold Run FAILED'."""
    xa, xb = 0, 650
    n.create_label(pg, f"Scheduled by 'Trigger Gold Build' (cron '{CRON_GOLD}'). Each step is one PostgreSQL "
                       "function = one transaction.\nDimensions are loaded before facts (left column down, right "
                       "column up); any failure marks the run FAILED in etl.pipeline_run and stops the chain.",
                   0, -180, 1002, 70)
    fail = n.create_port(pg, "OUTPUT_PORT", "Processing failures", 1530, 1900)["id"]
    fun_pre = n.create_funnel(pg, -700, 300)["id"]     # failures before a run id exists
    fun_a = n.create_funnel(pg, -480, 1100)["id"]      # failures of column A steps
    fun_b = n.create_funnel(pg, 1350, 800)["id"]       # failures of column B steps
    mark_failed = n.create_processor(pg, "ExecuteSQLRecord", "Mark Gold Run FAILED", 1480, 1300, {
        "Database Connection Pooling Service": cs["tgt"], "Record Writer": cs["w_plain"],
        "SQL Query": "SELECT * FROM etl.fail_run(${run.id}, "
                     "'${executesql.error.message:replace(\"'\", \"''\")}')"},
        comments="Records the error on the run so failed builds are visible in etl.pipeline_run.")["id"]

    trigger = n.create_processor(pg, "GenerateFlowFile", "Trigger Gold Build", xa, 0, {
        "Custom Text": "gold build", "Data Format": "Text"},
        schedule={"strategy": "CRON_DRIVEN", "period": CRON_GOLD},
        comments="Starts one gold build. Use 'Run Once' for an on-demand build.")["id"]
    start = n.create_processor(pg, "ExecuteSQLRecord", "Start Gold Run", xa, ROW, {
        "Database Connection Pooling Service": cs["tgt"], "Record Writer": cs["w_plain"],
        "SQL Query": "SELECT * FROM etl.start_run('gold_build', #{gold.full.refresh})"},
        comments="Opens a row in etl.pipeline_run and returns its run_id.")["id"]
    run_id = n.create_processor(pg, "EvaluateJsonPath", "Capture run.id", xa, 2 * ROW, {
        "Destination": "flowfile-attribute", "Return Type": "scalar", "run.id": "$[0].run_id"})["id"]
    n.connect(pg, proc(pg, trigger), proc(pg, start), ["success"])
    n.connect(pg, proc(pg, start), proc(pg, run_id), ["success"])
    n.connect(pg, proc(pg, start), funnel(pg, fun_pre), ["failure"])
    n.connect(pg, proc(pg, run_id), funnel(pg, fun_pre), ["failure", "unmatched"])

    # column A (down): prepare + 5 dimensions; column B (up): 3 facts + finalize
    positions = [(xa, (3 + i) * ROW) for i in range(6)] + [(xb, (8 - i) * ROW) for i in range(4)]
    prev, prev_rel = run_id, ["matched"]
    for (name, fn, comment), (x, y) in zip(GOLD_STEPS, positions):
        args = "${run.id}, #{gold.full.refresh}" if fn == "etl.gold_prepare" else "${run.id}"
        step = n.create_processor(pg, "ExecuteSQLRecord", name, x, y, {
            "Database Connection Pooling Service": cs["tgt"], "Record Writer": cs["w_plain"],
            "SQL Query": f"SELECT * FROM {fn}({args})"}, comments=comment)["id"]
        n.connect(pg, proc(pg, prev), proc(pg, step), prev_rel)
        n.connect(pg, proc(pg, step), funnel(pg, fun_a if x == xa else fun_b), ["failure"])
        prev, prev_rel = step, ["success"]

    finish = n.create_processor(pg, "ExecuteSQLRecord", "Complete Gold Run", xb, 4 * ROW, {
        "Database Connection Pooling Service": cs["tgt"], "Record Writer": cs["w_plain"],
        "SQL Query": "SELECT * FROM etl.finish_run(${run.id})"},
        comments="Marks the run SUCCEEDED and returns its duration and step count.")["id"]
    done = n.create_processor(pg, "LogMessage", "Log Gold Run Result", xb, 3 * ROW, {
        "Log Level": "info", "Log Prefix": "[ABC Hub ETL] ",
        "Log Message": "gold build run ${run.id} succeeded"}, auto_terminate=["success"])["id"]
    n.connect(pg, proc(pg, prev), proc(pg, finish), prev_rel)
    n.connect(pg, proc(pg, finish), proc(pg, done), ["success"])
    n.connect(pg, proc(pg, finish), funnel(pg, fun_b), ["failure"])

    n.connect(pg, funnel(pg, fun_b), proc(pg, mark_failed))
    n.connect(pg, funnel(pg, fun_a), proc(pg, mark_failed), bends=[(-456, 1800), (1656, 1800)])
    n.connect(pg, funnel(pg, fun_pre), port(pg, fail, "OUTPUT_PORT"), bends=[(-676, 1950), (1500, 1950)])
    n.connect(pg, proc(pg, mark_failed), port(pg, fail, "OUTPUT_PORT"), ["success", "failure"])


def build_errors(n, pg, cs):
    rej = n.create_port(pg, "INPUT_PORT", "Rejected records", 55, 0)["id"]
    fail = n.create_port(pg, "INPUT_PORT", "Processing failures", 955, 0)["id"]
    fun = n.create_funnel(pg, 600, 700)["id"]
    n.create_label(pg, "REJECTED RECORDS (validation failures, duplicates): written to a JSON file AND to "
                       "etl.rejected_record (one row per record), then a WARN bulletin", 0, -120, 800, 60)
    n.create_label(pg, "PROCESSING FAILURES (DB errors, bad SQL, retries exhausted): written to a file and "
                       "logged at ERROR level (raises a bulletin)", 900, -120, 800, 60)
    name_rej = n.create_processor(pg, "UpdateAttribute", "Name rejected-records file", 0, 180, {
        "filename": "${source.table}_${reject.reason}_${now():format('yyyyMMdd_HHmmss')}_${uuid}.json"})["id"]
    file_rej = n.create_processor(pg, "PutFile", "Write rejected records to log folder", 0, 180 + ROW, {
        "Directory": "#{log.directory}/rejected/${source.table}", "Conflict Resolution Strategy": "replace",
        "Create Missing Directories": "true"})["id"]
    wrap = n.create_processor(pg, "ReplaceText", "Wrap records as audit SQL", 0, 180 + 2 * ROW, {
        "Replacement Strategy": "Surround", "Evaluation Mode": "Entire text", "Maximum Buffer Size": "50 MB",
        "Text to Prepend": "SELECT * FROM etl.log_rejected_records('${batch.id}', '${source.table}', "
                           "'${reject.reason}', $vd$ ${validation.details} $vd$, $pl$",
        "Text to Append": "$pl$::jsonb, '${uuid}')"},
        comments="Surrounds the JSON array with a call of etl.log_rejected_records. Dollar quoting ($pl$) "
                 "makes any quotes inside the records safe.")["id"]
    db_rej = n.create_processor(pg, "ExecuteSQLRecord", "Record rejects in etl.rejected_record", 0, 180 + 3 * ROW, {
        "Database Connection Pooling Service": cs["tgt"], "Record Writer": cs["w_plain"]},
        comments="No SQL Query property set: executes the SQL in the FlowFile content.")["id"]
    warn = n.create_processor(pg, "LogMessage", "Log rejected-records warning", 0, 180 + 4 * ROW, {
        "Log Level": "warn", "Log Prefix": "[ABC Hub ETL] ",
        "Log Message": "${source.table}: rejected records (${reject.reason}) - batch ${batch.id}. "
                       "See etl.rejected_record and #{log.directory}/rejected"},
        auto_terminate=["success"], bulletin="WARN")["id"]

    name_fail = n.create_processor(pg, "UpdateAttribute", "Name failed-FlowFile file", 900, 180, {
        "filename": "${source.table:replaceNull('pipeline')}_${now():format('yyyyMMdd_HHmmss')}_${uuid}.txt"})["id"]
    file_fail = n.create_processor(pg, "PutFile", "Write failed FlowFile to log folder", 900, 180 + ROW, {
        "Directory": "#{log.directory}/failed/${source.table:replaceNull('pipeline')}",
        "Conflict Resolution Strategy": "replace", "Create Missing Directories": "true"})["id"]
    log_fail = n.create_processor(pg, "LogAttribute", "Log failed FlowFile (ERROR)", 900, 180 + 2 * ROW, {
        "Log Level": "error", "Log Payload": "false", "Log Prefix": "ABC Hub ETL FAILURE",
        "Attributes to Log Regular Expression": "source\\.table|batch\\.id|run\\.id|.*error.*|filename|uuid"},
        auto_terminate=["success"], bulletin="ERROR")["id"]

    n.connect(pg, port(pg, rej, "INPUT_PORT"), proc(pg, name_rej))
    n.connect(pg, proc(pg, name_rej), proc(pg, file_rej), ["success"])
    n.connect(pg, proc(pg, file_rej), proc(pg, wrap), ["success"])
    n.connect(pg, proc(pg, wrap), proc(pg, db_rej), ["success"])
    n.connect(pg, proc(pg, db_rej), proc(pg, warn), ["success"])
    n.connect(pg, port(pg, fail, "INPUT_PORT"), proc(pg, name_fail))
    n.connect(pg, proc(pg, name_fail), proc(pg, file_fail), ["success"])
    n.connect(pg, proc(pg, file_fail), proc(pg, log_fail), ["success", "failure"])
    # A failure inside the reject path is itself a processing failure.
    for p in (file_rej, wrap, db_rej):
        n.connect(pg, proc(pg, p), funnel(pg, fun), ["failure"])
    n.connect(pg, funnel(pg, fun), proc(pg, name_fail))


def wire_groups(n, top, pgs):
    def ports_of(pg_id, kind):
        path = "input-ports" if kind == "INPUT_PORT" else "output-ports"
        return {p["component"]["name"]: p["id"] for p in n.get(f"/process-groups/{pg_id}/{path}")[
            "inputPorts" if kind == "INPUT_PORT" else "outputPorts"]}

    def link(src_pg, src_name, dst_pg, dst_name, bends=None):
        s = ports_of(pgs[src_pg], "OUTPUT_PORT")[src_name]
        d = ports_of(pgs[dst_pg], "INPUT_PORT")[dst_name]
        n.connect(top, {"id": s, "groupId": pgs[src_pg], "type": "OUTPUT_PORT"},
                  {"id": d, "groupId": pgs[dst_pg], "type": "INPUT_PORT"}, name=src_name, bends=bends)

    link("extract", "Extracted batches", "bronze", "Extracted batches")
    link("extract", "Extracted batches", "cleanse", "Extracted batches")
    link("cleanse", "Clean batches", "silver", "Clean batches")
    link("cleanse", "Rejected records", "errors", "Rejected records", bends=[(900, 650)])
    link("cleanse", "Processing failures", "errors", "Processing failures", bends=[(1130, 560)])
    link("bronze", "Processing failures", "errors", "Processing failures", bends=[(1180, -60), (1180, 600)])
    link("silver", "Processing failures", "errors", "Processing failures")
    link("gold", "Processing failures", "errors", "Processing failures", bends=[(1800, -200), (1800, 880)])


# ---------------------------------------------------------------------------
# Start / export
# ---------------------------------------------------------------------------
def start_flow(n, top, schedules=True):
    """Starts every processor and port. The extracts and the gold trigger are
    cron-driven, so once running they fire on their schedules; run_pipeline.py
    can still trigger an immediate run. schedules=False leaves those source
    processors stopped (event-driven stages only)."""
    for p in n.get(f"/process-groups/{top}/processors?includeDescendantGroups=true")["processors"]:
        t = p["component"]["type"].split(".")[-1]
        if not schedules and t in ("QueryDatabaseTableRecord", "GenerateFlowFile"):
            continue
        n.set_processor_state(p["id"], "RUNNING")
    for pg in n.get(f"/process-groups/{top}/process-groups")["processGroups"]:
        for key, path in (("inputPorts", "input-ports"), ("outputPorts", "output-ports")):
            for prt in n.get(f"/process-groups/{pg['id']}/{path}")[key]:
                n.put(f"/{path}/{prt['id']}/run-status", {"revision": {"version": prt["revision"]["version"]},
                                                          "state": "RUNNING"})


def export(n, top):
    flow = n.download_flow(top)
    with open(EXPORT_PATH, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(flow, fh, indent=2)
        fh.write("\n")
    print(f"  exported flow definition -> {EXPORT_PATH}")


def find_top(n):
    for pg in n.get(f"/flow/process-groups/{n.root_id()}")["processGroupFlow"]["flow"]["processGroups"]:
        if pg["component"]["name"] == FLOW_NAME:
            return pg["id"]
    raise NiFiError(f"'{FLOW_NAME}' not found - build it first")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--nifi", default="http://127.0.0.1:8080/nifi-api")
    ap.add_argument("--export-only", action="store_true")
    ap.add_argument("--no-start", action="store_true", help="build but leave every processor stopped")
    ap.add_argument("--no-schedules", action="store_true",
                    help="start the event-driven stages but leave the cron-driven extracts / gold trigger stopped")
    args = ap.parse_args()
    n = NiFi(args.nifi)
    if args.export_only:
        export(n, find_top(n))
        return
    print("Building flow ...")
    top = build(n)
    errors = n.processor_validation_errors(top)
    if errors:
        print("Invalid processors:\n" + json.dumps(errors, indent=2))
        sys.exit(1)
    print("  all processors valid")
    if not args.no_start:
        start_flow(n, top, schedules=not args.no_schedules)
        print("  flow started" + ("" if args.no_schedules else
              " (extracts and gold build now run on their cron schedules)"))
    export(n, top)


if __name__ == "__main__":
    main()
