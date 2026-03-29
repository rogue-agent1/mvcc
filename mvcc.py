#!/usr/bin/env python3
"""MVCC — multi-version concurrency control."""
import sys, time

class Version:
    def __init__(self, value, txn_id, ts):
        self.value = value
        self.txn_id = txn_id
        self.created = ts
        self.deleted = float('inf')

class MVCCStore:
    def __init__(self):
        self.versions = {}  # key -> [Version]
        self.txn_counter = 0
    def begin(self):
        self.txn_counter += 1
        return self.txn_counter
    def read(self, key, txn_id):
        if key not in self.versions: return None
        for v in reversed(self.versions[key]):
            if v.created <= txn_id and v.deleted > txn_id:
                return v.value
        return None
    def write(self, key, value, txn_id):
        if key not in self.versions:
            self.versions[key] = []
        for v in self.versions[key]:
            if v.deleted == float('inf') and v.created < txn_id:
                v.deleted = txn_id
        self.versions[key].append(Version(value, txn_id, txn_id))
    def delete(self, key, txn_id):
        if key in self.versions:
            for v in self.versions[key]:
                if v.deleted == float('inf'):
                    v.deleted = txn_id

if __name__ == "__main__":
    store = MVCCStore()
    t1 = store.begin()
    store.write("x", 10, t1)
    store.write("y", 20, t1)
    t2 = store.begin()
    store.write("x", 30, t2)
    t3 = store.begin()
    print(f"T1 sees x={store.read('x', t1)}, y={store.read('y', t1)}")
    print(f"T2 sees x={store.read('x', t2)}, y={store.read('y', t2)}")
    print(f"T3 sees x={store.read('x', t3)}, y={store.read('y', t3)}")
    print(f"\nVersions of x: {len(store.versions.get('x', []))}")
    for v in store.versions.get("x", []):
        print(f"  value={v.value}, created=T{v.created}, deleted={'inf' if v.deleted==float('inf') else f'T{v.deleted}'}")
