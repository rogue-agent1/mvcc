#!/usr/bin/env python3
"""mvcc - Multi-Version Concurrency Control for snapshot isolation."""
import sys

class MVCC:
    def __init__(self):
        self.versions = {}  # key -> [(txn_id, value, deleted)]
        self.txn_counter = 0
        self.active = set()
    def begin(self):
        self.txn_counter += 1
        txn_id = self.txn_counter
        snapshot = frozenset(self.active)
        self.active.add(txn_id)
        return txn_id, snapshot
    def read(self, key, txn_id, snapshot):
        if key not in self.versions:
            return None
        for vid, value, deleted in reversed(self.versions[key]):
            if vid <= txn_id and vid not in snapshot:
                return None if deleted else value
        return None
    def write(self, key, value, txn_id):
        if key not in self.versions:
            self.versions[key] = []
        self.versions[key].append((txn_id, value, False))
    def delete(self, key, txn_id):
        if key not in self.versions:
            self.versions[key] = []
        self.versions[key].append((txn_id, None, True))
    def commit(self, txn_id):
        self.active.discard(txn_id)
    def rollback(self, txn_id):
        self.active.discard(txn_id)
        for key in self.versions:
            self.versions[key] = [(v, val, d) for v, val, d in self.versions[key] if v != txn_id]

def test():
    db = MVCC()
    t1, s1 = db.begin()
    db.write("x", 10, t1)
    db.commit(t1)
    t2, s2 = db.begin()
    assert db.read("x", t2, s2) == 10
    t3, s3 = db.begin()
    db.write("x", 20, t3)
    # t2 still sees 10 (snapshot isolation)
    assert db.read("x", t2, s2) == 10
    db.commit(t3)
    # t2 still sees old value
    assert db.read("x", t2, s2) == 10
    t4, s4 = db.begin()
    assert db.read("x", t4, s4) == 20
    # rollback
    t5, s5 = db.begin()
    db.write("y", 99, t5)
    db.rollback(t5)
    t6, s6 = db.begin()
    assert db.read("y", t6, s6) is None
    print("OK: mvcc")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test()
    else:
        print("Usage: mvcc.py test")
