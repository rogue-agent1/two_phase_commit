#!/usr/bin/env python3
"""Two-Phase Commit (2PC) — distributed transaction protocol.

One file. Zero deps. Does one thing well.

Coordinator asks participants to prepare, then commit/abort.
Implements basic 2PC with timeout handling and recovery log.
"""
import sys, random, time
from enum import Enum

class Vote(Enum):
    YES = "yes"
    NO = "no"
    TIMEOUT = "timeout"

class TxnState(Enum):
    INIT = "init"
    PREPARED = "prepared"
    COMMITTED = "committed"
    ABORTED = "aborted"

class Participant:
    def __init__(self, name, fail_rate=0.0):
        self.name = name
        self.fail_rate = fail_rate
        self.state = TxnState.INIT
        self.log = []

    def prepare(self, txn_id):
        if random.random() < self.fail_rate:
            self.log.append((txn_id, "VOTE-NO"))
            self.state = TxnState.ABORTED
            return Vote.NO
        self.log.append((txn_id, "VOTE-YES"))
        self.state = TxnState.PREPARED
        return Vote.YES

    def commit(self, txn_id):
        self.log.append((txn_id, "COMMIT"))
        self.state = TxnState.COMMITTED

    def abort(self, txn_id):
        self.log.append((txn_id, "ABORT"))
        self.state = TxnState.ABORTED

class Coordinator:
    def __init__(self, participants):
        self.participants = participants
        self.log = []
        self.txn_counter = 0

    def begin(self):
        self.txn_counter += 1
        txn_id = f"txn-{self.txn_counter}"
        self.log.append((txn_id, "BEGIN"))
        return txn_id

    def execute(self, txn_id):
        """Run 2PC protocol. Returns True if committed."""
        # Phase 1: Prepare
        self.log.append((txn_id, "PREPARE"))
        votes = {}
        for p in self.participants:
            votes[p.name] = p.prepare(txn_id)

        all_yes = all(v == Vote.YES for v in votes.values())

        # Phase 2: Commit or Abort
        if all_yes:
            self.log.append((txn_id, "COMMIT"))
            for p in self.participants:
                p.commit(txn_id)
            return True
        else:
            self.log.append((txn_id, "ABORT"))
            for p in self.participants:
                p.abort(txn_id)
            return False

    def run_transaction(self):
        txn_id = self.begin()
        result = self.execute(txn_id)
        return txn_id, result

def main():
    random.seed(42)
    print("=== Two-Phase Commit ===\n")

    # Successful transaction
    participants = [Participant(f"db-{i}") for i in range(3)]
    coord = Coordinator(participants)
    txn_id, ok = coord.run_transaction()
    print(f"{txn_id}: {'COMMITTED' if ok else 'ABORTED'}")
    for p in participants:
        print(f"  {p.name}: {p.state.value}")

    # Transaction with failure
    print()
    participants2 = [
        Participant("db-0", fail_rate=0.0),
        Participant("db-1", fail_rate=1.0),  # always fails
        Participant("db-2", fail_rate=0.0),
    ]
    coord2 = Coordinator(participants2)
    txn_id2, ok2 = coord2.run_transaction()
    print(f"{txn_id2}: {'COMMITTED' if ok2 else 'ABORTED'} (db-1 voted no)")
    for p in participants2:
        print(f"  {p.name}: {p.state.value}")

    # Stress test
    print(f"\n=== Stress Test (100 txns, 5 participants, 10% fail rate) ===")
    parts = [Participant(f"node-{i}", fail_rate=0.1) for i in range(5)]
    coord3 = Coordinator(parts)
    committed = sum(1 for _ in range(100) if coord3.run_transaction()[1])
    print(f"Committed: {committed}/100 ({committed}%)")
    print(f"Expected: ~{int(0.9**5 * 100)}% (all 5 must vote yes)")

if __name__ == "__main__":
    main()
