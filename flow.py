import copy
import hashlib
import json
import sqlite3


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def owner(key, width):
    return sum(key.encode("utf-8")) % width


def contribution(rows, fact_id):
    fact = rows["readings"].get(fact_id)
    if fact is None:
        return None
    device = rows["devices"].get(fact["device"])
    site = None if device is None else rows["sites"].get(device["site"])
    if site is None:
        return None
    return [fact_id, fact["day"], site["zone"], fact["amount"]]


class Journal:
    def __init__(self):
        self.db = sqlite3.connect(":memory:")
        self.db.executescript("""
        CREATE TABLE receipts(token TEXT PRIMARY KEY, digest TEXT NOT NULL,
                              packet TEXT NOT NULL);
        CREATE TABLE bag(row TEXT PRIMARY KEY, weight INTEGER NOT NULL);
        """)

    def accept(self, packet):
        token = canonical(packet["token"])
        body = canonical(packet)
        old = self.db.execute("SELECT digest FROM receipts WHERE token=?", (token,)).fetchone()
        if old:
            if old[0] != digest(packet):
                raise ValueError("TOKEN_CONTENT_CONFLICT")
            return False
        with self.db:
            self.db.execute("INSERT INTO receipts VALUES(?,?,?)", (token, digest(packet), body))
        return True

    def contains(self, token):
        return self.db.execute("SELECT 1 FROM receipts WHERE token=?", (canonical(token),)).fetchone() is not None

    def materialize(self, packet):
        with self.db:
            for row, diff in packet["changes"]:
                key = canonical(row)
                previous = self.db.execute("SELECT weight FROM bag WHERE row=?", (key,)).fetchone()
                value = (previous[0] if previous else 0) + diff
                self.db.execute("DELETE FROM bag WHERE row=?", (key,))
                if value:
                    self.db.execute("INSERT INTO bag VALUES(?,?)", (key, value))

    def image(self):
        return [[json.loads(row), weight] for row, weight in self.db.execute("SELECT row,weight FROM bag ORDER BY row")]

    def restore(self, image):
        with self.db:
            self.db.execute("DELETE FROM bag")
            self.db.executemany("INSERT INTO bag VALUES(?,?)", [(canonical(row), weight) for row, weight in image])

    def receipts(self):
        return [json.loads(row[0]) for row in self.db.execute("SELECT packet FROM receipts ORDER BY token")]


class Flow:
    def __init__(self, transactions, width=2):
        self.transactions = copy.deepcopy(transactions)
        self.width = width
        self.epoch = 0
        self.cut = 0
        self.sent = [0] * width
        self.workers = [self.new_worker() for _ in range(width)]
        self.inbox = [[] for _ in range(width)]
        self.sink = Journal()
        self.sink_inbox = []
        self.checkpoints = {}
        self.log = []

    @staticmethod
    def new_worker():
        return {"tables": {"readings": {}, "devices": {}, "sites": {}},
                "applied": 0, "out_seq": 0, "outbox": []}

    def record(self, event, **data):
        self.log.append(dict(n=len(self.log), event=event, epoch=self.epoch, **data))

    def send(self, through):
        if not self.cut <= through <= len(self.transactions):
            raise ValueError("SOURCE_CUT")
        for txn in self.transactions[self.cut:through]:
            if txn["lsn"] != self.cut + 1:
                raise ValueError("SOURCE_GAP")
            for index, op in enumerate(txn["ops"]):
                targets = [owner(op["id"], self.width)] if op["table"] == "readings" else range(self.width)
                for shard in targets:
                    self.sent[shard] += 1
                    msg = {"seq": self.sent[shard], "lsn": txn["lsn"], "index": index, "op": copy.deepcopy(op)}
                    self.inbox[shard].append(msg)
                    self.record("source.accept", shard=shard, seq=msg["seq"], lsn=msg["lsn"], index=index)
            self.cut = txn["lsn"]

    def deliver(self, shard, count=None):
        queue = self.inbox[shard]
        limit = len(queue) if count is None else min(count, len(queue))
        worker = self.workers[shard]
        for _ in range(limit):
            msg = queue.pop(0)
            if msg["seq"] != worker["applied"] + 1:
                raise ValueError("CHANNEL_GAP")
            tables = worker["tables"]
            op = msg["op"]
            table, key = op["table"], op["id"]
            if table == "readings":
                affected = [key]
            elif table == "devices":
                affected = [fid for fid, fact in tables["readings"].items() if fact["device"] == key]
            else:
                devices = {did for did, device in tables["devices"].items() if device["site"] == key}
                affected = [fid for fid, fact in tables["readings"].items() if fact["device"] in devices]
            before = {fid: contribution(tables, fid) for fid in affected}
            if op["value"] is None:
                tables[table].pop(key, None)
            else:
                tables[table][key] = copy.deepcopy(op["value"])
            changes = []
            for fid in sorted(affected):
                after = contribution(tables, fid)
                if before[fid] != after:
                    if before[fid] is not None:
                        changes.append([before[fid], -1])
                    if after is not None:
                        changes.append([after, 1])
            worker["applied"] = msg["seq"]
            if changes:
                worker["out_seq"] += 1
                packet = {"token": [self.epoch, shard, worker["out_seq"]],
                          "cause": [msg["lsn"], msg["index"], msg["seq"]], "changes": changes}
                worker["outbox"].append(packet)
            self.record("join.apply", shard=shard, seq=msg["seq"], lsn=msg["lsn"], changes=changes)

    def dispatch(self):
        for worker in self.workers:
            for packet in worker["outbox"]:
                fresh = self.sink.accept(packet)
                if fresh:
                    self.sink_inbox.append(copy.deepcopy(packet))
                self.record("sink.accept", token=packet["token"], fresh=fresh, cause=packet["cause"])

    def commit(self, count=None):
        limit = len(self.sink_inbox) if count is None else min(count, len(self.sink_inbox))
        for _ in range(limit):
            packet = self.sink_inbox.pop(0)
            self.sink.materialize(packet)
            self.record("sink.apply", token=packet["token"], changes=packet["changes"])

    def drain(self):
        for shard in range(self.width):
            self.deliver(shard)
        self.dispatch()
        self.commit()

    def checkpoint(self, name):
        snapshots = copy.deepcopy(self.workers)
        for worker in snapshots:
            worker["outbox"] = [p for p in worker["outbox"] if not self.sink.contains(p["token"])]
        body = {"cut": self.cut, "sent": list(self.sent), "workers": snapshots, "bag": self.sink.image()}
        manifest = {"name": name, "epoch": self.epoch, "hash": digest(body), "body": body}
        self.checkpoints[name] = copy.deepcopy(manifest)
        self.record("checkpoint.publish", name=name, hash=manifest["hash"], cut=self.cut,
                    sent=list(self.sent), applied=[w["applied"] for w in snapshots],
                    kept=[len(w["outbox"]) for w in snapshots], bag_digest=digest(body["bag"]))
        return manifest

    def recover(self, name):
        cp = copy.deepcopy(self.checkpoints[name])
        if cp["hash"] != digest(cp["body"]):
            raise ValueError("CHECKPOINT_CHECKSUM")
        self.epoch += 1
        body = cp["body"]
        self.cut, self.sent = body["cut"], body["sent"]
        self.workers = body["workers"]
        self.sink.restore(body["bag"])
        self.inbox = [[] for _ in range(self.width)]
        self.sink_inbox = []
        self.record("restore", name=name, cut=self.cut, checkpoint_epoch=cp["epoch"],
                    receipts=len(self.sink.receipts()))

    def evidence(self):
        return {"cut": self.cut, "epoch": self.epoch, "bag": self.sink.image(),
                "workers": copy.deepcopy(self.workers), "receipts": self.sink.receipts(),
                "checkpoints": copy.deepcopy(self.checkpoints), "trace": copy.deepcopy(self.log)}
