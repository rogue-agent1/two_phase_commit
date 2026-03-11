#!/usr/bin/env python3
"""Two-Phase Commit protocol simulation."""
import random, sys

class Participant:
    def __init__(self, name, fail_prob=0.0):
        self.name, self.fail_prob = name, fail_prob
        self.state = 'INIT'; self.data = None
    def prepare(self, data):
        self.data = data
        if random.random() < self.fail_prob:
            self.state = 'ABORT'; return False
        self.state = 'READY'; return True
    def commit(self): self.state = 'COMMITTED'; return True
    def abort(self): self.state = 'ABORTED'; self.data = None; return True

class Coordinator:
    def __init__(self, participants): self.participants = participants
    def execute(self, data):
        print(f"Phase 1: PREPARE ({data})")
        votes = []
        for p in self.participants:
            vote = p.prepare(data)
            print(f"  {p.name}: {'READY' if vote else 'ABORT'}")
            votes.append(vote)
        if all(votes):
            print("Phase 2: COMMIT")
            for p in self.participants: p.commit(); print(f"  {p.name}: COMMITTED")
            return True
        else:
            print("Phase 2: ABORT")
            for p in self.participants:
                if p.state == 'READY': p.abort()
                print(f"  {p.name}: {p.state}")
            return False

if __name__ == "__main__":
    random.seed(42)
    ps = [Participant("DB1"), Participant("DB2"), Participant("DB3", fail_prob=0.3)]
    coord = Coordinator(ps)
    for i in range(3):
        print(f"\n--- Transaction {i+1} ---")
        result = coord.execute(f"UPDATE balance SET amount={i*100}")
        print(f"Result: {'COMMITTED' if result else 'ABORTED'}")
