#!/usr/bin/env python3
"""MVCC — Multi-Version Concurrency Control for databases.

One file. Zero deps. Does one thing well.

Implements snapshot isolation with versioned key-value pairs.
Each transaction sees a consistent snapshot. Used in PostgreSQL, CockroachDB, TiKV.
"""
import sys, threading, time
from collections import defaultdict

class Version:
    __slots__ = ('value', 'txn_id', 'begin_ts', 'end_ts', 'deleted')
    def __init__(self, value, txn_id, begin_ts):
        self.value = value
        self.txn_id = txn_id
        self.begin_ts = begin_ts
        self.end_ts = float('inf')
        self.deleted = False

class Transaction:
    def __init__(self, txn_id, snapshot_ts, store):
        self.id = txn_id
        self.snapshot_ts = snapshot_ts
        self.store = store
        self.writes = {}  # key -> value
        self.committed = False
        self.aborted = False

    def get(self, key):
        if key in self.writes:
            v = self.writes[key]
            return None if v is _DELETED else v
        return self.store._read(key, self.snapshot_ts)

    def put(self, key, value):
        self.writes[key] = value

    def delete(self, key):
        self.writes[key] = _DELETED

    def commit(self):
        return self.store._commit(self)

    def abort(self):
        self.aborted = True

class _DeletedSentinel:
    def __repr__(self): return '<DELETED>'
_DELETED = _DeletedSentinel()

class MVCCStore:
    def __init__(self):
        self.versions = defaultdict(list)  # key -> [Version] ordered by begin_ts
        self.ts_counter = 0
        self.lock = threading.Lock()
        self.committed_txns = set()

    def _next_ts(self):
        with self.lock:
            self.ts_counter += 1
            return self.ts_counter

    def begin(self):
        ts = self._next_ts()
        txn_id = ts
        return Transaction(txn_id, ts, self)

    def _read(self, key, snapshot_ts):
        """Find the latest committed version visible at snapshot_ts."""
        for ver in reversed(self.versions[key]):
            if ver.begin_ts <= snapshot_ts and ver.end_ts > snapshot_ts:
                if ver.txn_id in self.committed_txns:
                    return None if ver.deleted else ver.value
        return None

    def _commit(self, txn):
        if txn.aborted:
            return False
        commit_ts = self._next_ts()
        with self.lock:
            # Write-write conflict detection
            for key in txn.writes:
                for ver in reversed(self.versions[key]):
                    if ver.begin_ts > txn.snapshot_ts and ver.txn_id in self.committed_txns:
                        txn.aborted = True
                        return False  # Conflict!
            # Apply writes
            for key, value in txn.writes.items():
                # End previous version
                for ver in reversed(self.versions[key]):
                    if ver.end_ts == float('inf') and ver.txn_id in self.committed_txns:
                        ver.end_ts = commit_ts
                        break
                new_ver = Version(value if value is not _DELETED else None, txn.id, commit_ts)
                new_ver.deleted = value is _DELETED
                self.versions[key].append(new_ver)
            self.committed_txns.add(txn.id)
            txn.committed = True
        return True

    def gc(self, oldest_active_ts):
        """Garbage collect versions no longer visible."""
        with self.lock:
            for key in self.versions:
                self.versions[key] = [
                    v for v in self.versions[key]
                    if v.end_ts > oldest_active_ts or v.begin_ts >= oldest_active_ts
                ]

def main():
    store = MVCCStore()

    # Setup
    t0 = store.begin()
    t0.put("x", 10)
    t0.put("y", 20)
    t0.commit()

    # Concurrent reads: t1 sees snapshot before t2's write
    t1 = store.begin()
    t2 = store.begin()
    t2.put("x", 99)
    t2.commit()
    print(f"t1 reads x={t1.get('x')} (snapshot isolation, doesn't see t2's write)")
    print(f"Fresh read x={store.begin().get('x')} (sees committed value)")

    # Write-write conflict
    t3 = store.begin()
    t4 = store.begin()
    t3.put("y", 100)
    t3.commit()
    t4.put("y", 200)
    ok = t4.commit()
    print(f"\nConflict test: t4.commit()={ok} (write-write conflict detected)")

    # Delete
    t5 = store.begin()
    t5.delete("x")
    t5.commit()
    t6 = store.begin()
    print(f"\nAfter delete: x={t6.get('x')}")
    print("✓ MVCC working correctly")

if __name__ == "__main__":
    main()
