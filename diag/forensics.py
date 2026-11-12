#!/usr/bin/env python3
"""Read-only forensics for recovery-cut mismatches.

Input : one raw replay output produced by `incident.py replay ...`
        plus the spec JSON it ran (only the specific lost messages are
        looked up; the source log is never replayed in full).
Output: a JSON verdict derived solely from recovery-boundary messages,
        packets, receipts and worker/checkpoint state carried in evidence.

This script imports flow.contribution for local dependency replay of the
exact lost input messages only (CONTRACT.md allows evidence-local replay).
It never mutates flow.py, incident.py, the spec or the replay output.
"""
import copy
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from flow import contribution, canonical  # noqa: E402


def index_source(spec):
    by_lsn = {txn["lsn"]: txn["ops"] for txn in spec["transactions"]}
    return by_lsn


def events_before(trace, kind, n):
    return [e for e in trace if e["event"] == kind and e["n"] < n]


def analyze(spec, out):
    src = index_source(spec)
    width = spec["width"]
    trace = out["trace"]
    cps = [e for e in trace if e["event"] == "checkpoint.publish"]
    restores = [e for e in trace if e["event"] == "restore"]
    verdict = {"checkpoints": [], "recoveries": []}

    # ---- per-checkpoint boundary derivation (no full source replay) ----
    for cp_ev in cps:
        k = cp_ev["n"]
        name = cp_ev["name"]
        cp_body = out["checkpoints"][name]["body"]

        accepted, applied = set(), set()
        for e in events_before(trace, "sink.accept", k):
            if e["fresh"]:
                accepted.add(canonical(e["token"]))
        for e in events_before(trace, "sink.apply", k):
            applied.add(canonical(e["token"]))

        # Mechanism A: accepted by the durable receipt log, never materialized
        # into the bag before the checkpoint was published.
        lost_accepted = sorted(json.loads(t) for t in accepted - applied)

        # Mechanism B: source messages accepted into a shard inbox but never
        # applied by that worker before the checkpoint.
        sent_seq = {s: set() for s in range(width)}
        for e in events_before(trace, "source.accept", k):
            sent_seq[e["shard"]].add(e["seq"])
        lost_inputs = {}
        for s, worker in enumerate(cp_body["workers"]):
            gap = sorted(seq for seq in sent_seq[s] if seq > worker["applied"])
            lost_inputs[s] = gap

        # Local replay of only the lost messages on the checkpointed worker
        # state to derive their would-be signed output (dependency-local).
        latent = []
        for s, seqs in lost_inputs.items():
            worker = copy.deepcopy(cp_body["workers"][s])
            tables = worker["tables"]
            # reconstruct messages from source via (lsn, index) recorded at accept
            accepts = {(e["shard"], e["seq"]): e
                       for e in events_before(trace, "source.accept", k)}
            for seq in seqs:
                ev = accepts[(s, seq)]
                op = src[ev["lsn"]][ev["index"]]
                table, key = op["table"], op["id"]
                if table == "readings":
                    affected = [key]
                elif table == "devices":
                    affected = [fid for fid, fact in tables["readings"].items()
                                if fact["device"] == key]
                else:
                    devices = {did for did, dev in tables["devices"].items()
                               if dev["site"] == key}
                    affected = [fid for fid, fact in tables["readings"].items()
                                if fact["device"] in devices]
                before = {fid: contribution(tables, fid) for fid in affected}
                if op["value"] is None:
                    tables[table].pop(key, None)
                else:
                    tables[table][key] = copy.deepcopy(op["value"])
                emitted = []
                for fid in sorted(affected):
                    after = contribution(tables, fid)
                    if before[fid] != after:
                        if before[fid] is not None:
                            emitted.append([before[fid], -1])
                        if after is not None:
                            emitted.append([after, 1])
                joinable = [fid for fid in affected
                            if contribution(tables, fid) is not None
                            or before[fid] is not None]
                latent.append({"shard": s, "seq": seq, "lsn": ev["lsn"],
                               "index": ev["index"], "table": table, "id": key,
                               "would_emit": emitted,
                               "currently_observable_facts": joinable,
                               "dangling_affected": sorted(set(affected) - set(joinable))})

        # Would the next send on each shard trip CHANNEL_GAP?
        next_seq = {s: (cp_body["sent"][s] + 1) for s in range(width)}
        gap_risk = {s: cp_body["workers"][s]["applied"] + 1 != next_seq[s]
                    for s in range(width)}

        verdict["checkpoints"].append({
            "name": name, "at_event": k, "cut": cp_body["cut"],
            "hash": out["checkpoints"][name]["hash"],
            "accepted_not_materialized_tokens": lost_accepted,
            "lost_inflight_inputs": lost_inputs,
            "local_replay_of_lost_inputs": latent,
            "channel_gap_on_next_send": gap_risk,
            "retained_outbox_tokens": [pkt["token"] for w in cp_body["workers"]
                                       for pkt in w["outbox"]],
        })

    # ---- recovery outcomes: same checkpoint hash, different boundary use ----
    for r in restores:
        name = r["name"]
        verdict["recoveries"].append({
            "name": name, "epoch_after": r["epoch"],
            "checkpoint_hash": out["checkpoints"][name]["hash"],
            "receipts_surviving": r["receipts"],
            "note": "bag is reset to checkpoint image; receipts survive; "
                    "inbox and sink execution queue are discarded",
        })

    # ---- token-keyed net error across all recovery epochs ----
    # A sink.apply before a restore is rolled back by restore(bag image),
    # so membership in the sink.apply trace does NOT prove durable
    # materialization. Fold the surviving receipt packets (they carry full
    # packet bodies) and diff against the final bag: any discrepancy is the
    # Mechanism-A footprint, derived without replaying the source log.
    folded = {}
    receipt_packets = []
    for packet in out["receipts"]:
        receipt_packets.append(packet)
        for row, diff in packet["changes"]:
            key = canonical(row)
            folded[key] = folded.get(key, 0) + diff
    receipt_bag = sorted([[json.loads(k), w] for k, w in folded.items() if w],
                         key=lambda item: canonical(item[0]))
    final_bag = out["bag"]
    receipt_only_tokens = []
    if receipt_bag != final_bag:
        # Rows whose folded weight differs from the final bag are exactly the
        # Mechanism-A footprint; attribute packets that touch those rows.
        actual_w = {canonical(r): w for r, w in final_bag}
        diff_rows = {k for k, w in folded.items()
                     if w != 0 and actual_w.get(k, 0) != w}
        diff_rows |= {canonical(r) for r, w in final_bag
                      if folded.get(canonical(r), 0) != w}
        for packet in receipt_packets:
            if any(canonical(row) in diff_rows for row, _ in packet["changes"]):
                receipt_only_tokens.append(packet["token"])
    verdict["receipt_fold_vs_final_bag_equal"] = receipt_bag == final_bag
    verdict["mechanism_A_tokens_with_rolled_back_effect"] = receipt_only_tokens
    verdict["status"] = out["status"]
    verdict["runtime_error"] = out["runtime_error"]
    return verdict


def main(argv):
    spec = json.load(open(argv[0], encoding="utf-8"))
    out = json.load(open(argv[1], encoding="utf-8"))
    print(json.dumps(analyze(spec, out), sort_keys=True, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
