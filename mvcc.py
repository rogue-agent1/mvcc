#!/usr/bin/env python3
"""Multi-Version Concurrency Control database engine."""
import sys, time, threading
from collections import defaultdict

class Version:
    def __init__(self, value, txn_id, ts):
        self.value = value; self.txn_id = txn_id; self.ts = ts; self.deleted = value is None

class Transaction:
    _counter = 0; _lock = threading.Lock()
    def __init__(self, db):
        with Transaction._lock: Transaction._counter += 1; self.id = Transaction._counter
        self.db = db; self.start_ts = time.time(); self.writes = {}
        self.committed = False; self.aborted = False

    def get(self, key):
        if key in self.writes: return self.writes[key]
        versions = self.db.store.get(key, [])
        for v in reversed(versions):
            if v.ts <= self.start_ts and not v.deleted: return v.value
        return None

    def put(self, key, value): self.writes[key] = value

    def delete(self, key): self.writes[key] = None

    def commit(self):
        ts = time.time()
        with self.db.lock:
            for key, value in self.writes.items():
                self.db.store.setdefault(key, []).append(Version(value, self.id, ts))
        self.committed = True; return True

    def rollback(self): self.writes.clear(); self.aborted = True

class MVCCDatabase:
    def __init__(self):
        self.store = {}; self.lock = threading.Lock()
        self.stats = {"transactions": 0, "commits": 0, "rollbacks": 0}

    def begin(self):
        self.stats["transactions"] += 1; return Transaction(self)

    def gc(self, before_ts):
        with self.lock:
            removed = 0
            for key in list(self.store):
                versions = self.store[key]
                self.store[key] = [v for v in versions if v.ts >= before_ts]
                removed += len(versions) - len(self.store[key])
                if not self.store[key]: del self.store[key]
            return removed

    def snapshot(self, ts=None):
        ts = ts or time.time(); result = {}
        for key, versions in self.store.items():
            for v in reversed(versions):
                if v.ts <= ts:
                    if not v.deleted: result[key] = v.value
                    break
        return result

def main():
    print("=== MVCC Database ===\n")
    db = MVCCDatabase()
    tx1 = db.begin(); tx1.put("x", 10); tx1.put("y", 20); tx1.commit()
    print(f"Tx1 committed: x=10, y=20")
    ts_after_tx1 = time.time()
    tx2 = db.begin(); tx2.put("x", 30)
    tx3 = db.begin()
    print(f"Tx3 reads x={tx3.get('x')} (sees tx1, not uncommitted tx2)")
    tx2.commit(); print(f"Tx2 committed: x=30")
    print(f"Tx3 still reads x={tx3.get('x')} (snapshot isolation)")
    tx4 = db.begin()
    print(f"Tx4 reads x={tx4.get('x')} (sees tx2)")
    tx5 = db.begin(); tx5.put("z", 99); tx5.rollback()
    print(f"Tx5 rolled back, z={db.begin().get('z')}")
    print(f"\nSnapshot at tx1: {db.snapshot(ts_after_tx1)}")
    print(f"Current snapshot: {db.snapshot()}")

if __name__ == "__main__": main()
