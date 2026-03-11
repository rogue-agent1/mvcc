#!/usr/bin/env python3
"""MVCC — Multi-Version Concurrency Control for snapshot isolation."""
import threading

class Version:
    def __init__(self, value, txn_id, prev=None):
        self.value = value; self.txn_id = txn_id; self.prev = prev

class MVCCStore:
    def __init__(self):
        self.data = {}; self.lock = threading.Lock()
        self.next_txn = 1; self.active = set(); self.committed = {}
    def begin(self):
        with self.lock:
            txn_id = self.next_txn; self.next_txn += 1
            snapshot = frozenset(self.committed.keys())
            self.active.add(txn_id)
        return txn_id, snapshot
    def read(self, txn_id, snapshot, key):
        with self.lock:
            ver = self.data.get(key)
            while ver:
                if ver.txn_id == txn_id or ver.txn_id in snapshot: return ver.value
                ver = ver.prev
        return None
    def write(self, txn_id, key, value):
        with self.lock:
            old = self.data.get(key)
            self.data[key] = Version(value, txn_id, old)
    def commit(self, txn_id):
        with self.lock:
            self.active.discard(txn_id)
            self.committed[txn_id] = True
        return True
    def abort(self, txn_id):
        with self.lock:
            self.active.discard(txn_id)
            for key in list(self.data):
                ver = self.data[key]
                if ver and ver.txn_id == txn_id: self.data[key] = ver.prev

if __name__ == "__main__":
    store = MVCCStore()
    t1, s1 = store.begin(); store.write(t1, "x", 10); store.commit(t1)
    t2, s2 = store.begin(); t3, s3 = store.begin()
    store.write(t2, "x", 20)
    print(f"T3 reads x (snapshot): {store.read(t3, s3, 'x')}")  # sees 10
    store.commit(t2)
    print(f"T3 still sees: {store.read(t3, s3, 'x')}")  # still 10 (snapshot)
    t4, s4 = store.begin()
    print(f"T4 sees: {store.read(t4, s4, 'x')}")  # sees 20
