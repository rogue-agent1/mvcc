#!/usr/bin/env python3
"""mvcc - Multi-Version Concurrency Control engine."""
import argparse, threading

class Version:
    def __init__(self, value, txn_id, ts):
        self.value,self.txn_id,self.ts=value,txn_id,ts

class MVCCStore:
    def __init__(self):
        self.data={};self.txn_counter=0;self.ts_counter=0;self.active=set()
    def begin(self):
        self.txn_counter+=1;self.ts_counter+=1
        txn={"id":self.txn_counter,"start_ts":self.ts_counter,"writes":{},"reads":set()}
        self.active.add(txn["id"]);return txn
    def read(self, txn, key):
        txn["reads"].add(key)
        if key in txn["writes"]: return txn["writes"][key]
        if key not in self.data: return None
        for v in reversed(self.data[key]):
            if v.ts<=txn["start_ts"] and v.txn_id not in self.active: return v.value
        return None
    def write(self, txn, key, value): txn["writes"][key]=value
    def commit(self, txn):
        self.ts_counter+=1; commit_ts=self.ts_counter
        for key,value in txn["writes"].items():
            self.data.setdefault(key,[]).append(Version(value,txn["id"],commit_ts))
        self.active.discard(txn["id"]); return commit_ts
    def rollback(self, txn): self.active.discard(txn["id"])

def main():
    p=argparse.ArgumentParser(description="MVCC engine"); args=p.parse_args()
    store=MVCCStore()
    t1=store.begin(); store.write(t1,"x",10); store.write(t1,"y",20); store.commit(t1)
    print("T1: write x=10, y=20, commit")
    t2=store.begin(); t3=store.begin()
    print(f"T2 reads x={store.read(t2,'x')}")
    store.write(t3,"x",30); store.commit(t3)
    print("T3: write x=30, commit")
    print(f"T2 still reads x={store.read(t2,'x')} (snapshot isolation)")
    t4=store.begin()
    print(f"T4 reads x={store.read(t4,'x')} (sees T3's write)")
    store.commit(t2); store.commit(t4)
    print(f"\nVersion history for 'x': {[(v.value,v.ts) for v in store.data.get('x',[])]}")

if __name__=="__main__":
    main()
