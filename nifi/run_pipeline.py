"""
Runs the ABC Hub ETL pipeline on demand (the same thing as right-clicking the
scheduled processors in the NiFi UI and choosing "Run Once").

    python nifi/run_pipeline.py              # extract -> bronze/silver, then gold
    python nifi/run_pipeline.py --extract    # only the extraction stages
    python nifi/run_pipeline.py --gold       # only the gold build

In production the extracts and the gold trigger run on their cron schedules
(see build_flow.py); this script triggers an immediate run for demos, testing
and screenshots. Scheduled processors are paused for the trigger and then put
back on their schedule.
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from nifi_api import NiFi  # noqa: E402
from build_flow import find_top  # noqa: E402


def processors(n, top, short_type):
    return [p for p in n.get(f"/process-groups/{top}/processors?includeDescendantGroups=true")["processors"]
            if p["component"]["type"].endswith("." + short_type)]


def run_once(n, procs, label):
    """'Run Once' needs a stopped processor: a scheduled (running) one is paused,
    triggered once, and put back on its schedule."""
    print(f"==> {label}: triggering {len(procs)} processor(s)")
    was_running = [p["id"] for p in procs if p["component"]["state"] == "RUNNING"]
    for pid in was_running:
        n.set_processor_state(pid, "STOPPED")
    for pid in was_running:
        for _ in range(50):
            if n.get(f"/processors/{pid}")["status"]["aggregateSnapshot"]["activeThreadCount"] == 0:
                break
            time.sleep(0.2)
    for p in procs:
        n.set_processor_state(p["id"], "RUN_ONCE")
    return was_running


def restore_schedules(n, pids):
    """Put paused processors back on their cron schedule. Must happen after the
    single run has finished: a run-once processor that is still executing goes
    back to STOPPED when it completes, overriding an earlier RUNNING request."""
    for pid in pids:
        for _ in range(100):
            proc = n.get(f"/processors/{pid}")
            if proc["status"]["aggregateSnapshot"]["activeThreadCount"] == 0:
                break
            time.sleep(0.2)
        n.set_processor_state(pid, "RUNNING")
    stopped = [pid for pid in pids if n.get(f"/processors/{pid}")["component"]["state"] != "RUNNING"]
    if stopped:
        print(f"    WARNING: {len(stopped)} processor(s) could not be put back on schedule")


def wait(n, top, label, timeout):
    start = time.time()
    time.sleep(3)
    ok = n.wait_for_queues_empty(top, timeout=timeout)
    print(f"    {label} {'finished' if ok else 'TIMED OUT'} in {time.time() - start:.0f}s")
    return ok


def bulletins(n):
    board = n.get("/flow/bulletin-board?limit=20")["bulletinBoard"]["bulletins"]
    recent = [b["bulletin"] for b in board if b.get("bulletin")]
    for b in recent[-10:]:
        print(f"    [{b['level']}] {b['sourceName']}: {b['message'][:300]}")
    return recent


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nifi", default="http://127.0.0.1:8080/nifi-api")
    ap.add_argument("--extract", action="store_true", help="only run the extraction stages")
    ap.add_argument("--gold", action="store_true", help="only run the gold build")
    ap.add_argument("--timeout", type=int, default=900)
    args = ap.parse_args()
    run_extract = args.extract or not args.gold
    run_gold = args.gold or not args.extract

    n = NiFi(args.nifi)
    top = find_top(n)
    ok = True
    if run_extract:
        paused = run_once(n, processors(n, top, "QueryDatabaseTableRecord"), "extract (operational -> bronze + silver)")
        ok &= wait(n, top, "extract/validate/cleanse/load", args.timeout)
        restore_schedules(n, paused)
    if run_gold and ok:
        paused = run_once(n, processors(n, top, "GenerateFlowFile"), "gold build")
        ok &= wait(n, top, "gold build", args.timeout)
        restore_schedules(n, paused)
    print("==> recent bulletins (warnings / errors):")
    if not bulletins(n):
        print("    none")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
