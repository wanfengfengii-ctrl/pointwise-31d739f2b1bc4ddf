import copy
import json
import pathlib
import sys

from flow import Flow, canonical


MARKER = "RECOVERY_CUT_MISMATCH"


def expected(transactions, cut):
    tables = {"readings": {}, "devices": {}, "sites": {}}
    for txn in transactions[:cut]:
        for op in txn["ops"]:
            if op["value"] is None:
                tables[op["table"]].pop(op["id"], None)
            else:
                tables[op["table"]][op["id"]] = copy.deepcopy(op["value"])
    rows = []
    for fid, fact in tables["readings"].items():
        device = tables["devices"].get(fact["device"])
        if device is None:
            continue
        site = tables["sites"].get(device["site"])
        if site is not None:
            rows.append([[fid, fact["day"], site["zone"], fact["amount"]], 1])
    return sorted(rows, key=lambda item: canonical(item[0]))


def summary(bag):
    groups = {}
    for row, weight in bag:
        _, day, zone, amount = row
        key = (day, zone)
        current = groups.setdefault(key, [0, 0])
        current[0] += amount * weight
        current[1] += weight
    return [[day, zone, amount, count] for (day, zone), (amount, count) in sorted(groups.items()) if amount or count]


def run(spec, scenario):
    flow = Flow(spec["transactions"], spec["width"])
    error = None
    failed_step = None
    for index, step in enumerate(spec["scenarios"][scenario]):
        action, *args = step
        try:
            if action not in {"send", "deliver", "dispatch", "commit", "drain", "checkpoint", "recover"}:
                raise ValueError("UNKNOWN_ACTION")
            getattr(flow, action)(*args)
        except ValueError as exc:
            error, failed_step = str(exc), index
            flow.record("runtime.error", step=index, error=error)
            break
    result = flow.evidence()
    wanted = expected(spec["transactions"], flow.cut)
    result.update(scenario=scenario, expected=wanted, actual_summary=summary(result["bag"]),
                  expected_summary=summary(wanted), runtime_error=error, failed_step=failed_step)
    result["status"] = "OK" if result["bag"] == wanted and error is None else MARKER
    return result


def load(path):
    return json.loads(pathlib.Path(path).read_text(encoding="utf-8"))


def normal(spec):
    transactions = spec["transactions"]
    for width in (1, 2, 3):
        flow = Flow(transactions, width)
        for cut in range(1, len(transactions) + 1):
            flow.send(cut)
            flow.drain()
            flow.checkpoint("stable-" + str(cut))
            flow.recover("stable-" + str(cut))
            flow.drain()
            assert flow.sink.image() == expected(transactions, cut), (width, cut)
    # Also exercise tombstone exposure and re-creation independently of recovery.
    extra = copy.deepcopy(transactions)
    extra.extend([
        {"lsn": len(extra) + 1, "ops": [{"table": "sites", "id": "B", "value": None}]},
        {"lsn": len(extra) + 2, "ops": [{"table": "sites", "id": "B", "value": {"zone": "west"}}]}
    ])
    flow = Flow(extra)
    for cut in range(1, len(extra) + 1):
        flow.send(cut)
        flow.drain()
        assert flow.sink.image() == expected(extra, cut)
    print(canonical({"status": "NORMAL_REGRESSION_OK", "widths": [1, 2, 3], "cuts": len(transactions)}))


def main(argv):
    mode = argv[0] if argv else "demo"
    if mode in {"normal", "verify"}:
        spec = load("samples/case.json")
        normal(spec)
        if mode == "verify":
            outputs = [run(spec, "incident") for _ in range(2)]
            assert all(item["status"] == MARKER and item["runtime_error"] is None for item in outputs)
            assert outputs[0] == outputs[1]
            print(canonical({"status": "INCIDENT_REPRODUCED", "runs": [
                {"status": r["status"], "cut": r["cut"], "actual": r["bag"], "expected": r["expected"]} for r in outputs]}))
        return 0
    if mode not in {"replay", "demo"}:
        raise ValueError("MODE")
    path = argv[1] if len(argv) > 1 else "samples/case.json"
    scenario = argv[2] if len(argv) > 2 else "incident"
    output = run(load(path), scenario)
    print(canonical(output))
    return int(mode == "replay" and output["status"] != "OK")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
