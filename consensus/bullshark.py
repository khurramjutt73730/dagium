"""
DAGIUM Bullshark Module
Implements deterministic transaction ordering and finality via Bullshark consensus.
"""

import time
import logging
from typing import List, Dict, Set, Optional
from dataclasses import dataclass, field
from collections import defaultdict

logger = logging.getLogger(__name__)


dataclass
class QuorumCertificate:
    """Represents a Quorum Certificate (QC) for a round."""
    
    round_number: int
    signers: Set[str]  # validator IDs that signed
    timestamp: float = field(default_factory=time.time)
    
    def is_quorum(self, total_validators: int) -> bool:
        """Check if this QC has quorum (>2/3 of validators)."""
        return len(self.signers) > (2 * total_validators) // 3


dataclass
class OrderedTransaction:
    """Transaction with deterministic ordering information."""
    
    tx_hash: str
    tx_data: bytes
    round: int
    creator_id: str
    order_index: int
    finalized: bool = False


class BullsharkConsensus:
    """Bullshark consensus engine for deterministic ordering and finality."""
    
    def __init__(self, dag, validator_id: str, all_validators: List[str]):
        """Initialize Bullshark consensus."""
        
        Args:
            dag: TransactionDAG instance
            validator_id: This validator's ID
            all_validators: List of all validator IDs
        """
        self.dag = dag
        self.validator_id = validator_id
        self.all_validators = all_validators
        self.total_validators = len(all_validators)
        
        self.current_round = 0
        self.qc_chain: Dict[int, QuorumCertificate] = {}  # round → QC
        
        self.ordered_transactions: List[OrderedTransaction] = []
        self.finalized_rounds: Set[int] = set()
        
        self.finality_threshold = 2  # consecutive QCs needed for finality
        self.round_timeout = 0.8  # seconds
        self.last_round_advance = time.time()
    
    def advance_round(self):
        """Advance to the next round."""
        self.current_round += 1
        self.last_round_advance = time.time()
        logger.debug(f"Advanced to round {self.current_round}")
    
    def submit_quorum_certificate(self, round_number: int, signers: Set[str]) -> bool:
        """Submit a quorum certificate for a round."""
        
        Args:
            round_number: Round number
            signers: Set of validator IDs that signed
            
        Returns:
            True if QC is valid and stored
        """
        if round_number in self.qc_chain:
            logger.debug(f"QC already exists for round {round_number}")
            return False
        
        qc = QuorumCertificate(round_number=round_number, signers=signers)
        
        if not qc.is_quorum(self.total_validators):
            logger.warning(f"QC for round {round_number} does not have quorum")
            return False
        
        self.qc_chain[round_number] = qc
        logger.info(f"QC accepted for round {round_number} with {len(signers)} signers")
        
        # Check for finality
        self._check_finality()
        
        return True
    
    def _check_finality(self):
        """Check if we have achieved finality. Finality requires consecutive QCs."""
        sorted_rounds = sorted(self.qc_chain.keys())
        
        for i in range(len(sorted_rounds) - 1):
            current_round = sorted_rounds[i]
            next_round = sorted_rounds[i + 1]
            
            # Check if rounds are consecutive
            if next_round == current_round + 1:
                if current_round not in self.finalized_rounds:
                    self.finalized_rounds.add(current_round)
                    logger.info(f"Finality achieved for round {current_round}")
                    self._finalize_round(current_round)
    
    def _finalize_round(self, round_number: int):
        """Finalize all transactions in a round."""
        
        Args:
            round_number: Round to finalize
        """
        # Mark transactions in this round as finalized
        for tx in self.ordered_transactions:
            if tx.round == round_number and not tx.finalized:
                tx.finalized = True
        
        logger.debug(f"Round {round_number} finalized")
    
    def deterministically_order_vertices(self) -> List[OrderedTransaction]:
        """Deterministically order all DAG vertices. Ordering: Round → Validator ID → Transaction Hash"""
        
        Returns:
            List of OrderedTransaction objects
        """
        all_vertices = self.dag.get_all_vertices()
        
        # Create tuples for sorting: (round, creator_id, tx_hash)
        vertices_with_sort_keys = []
        for vertex in all_vertices:
            tx_hash = vertex.compute_hash()
            round_num = vertex.round_number if vertex.round_number else self.current_round
            sort_key = (round_num, vertex.creator_id, tx_hash)
            vertices_with_sort_keys.append((sort_key, vertex, tx_hash))
        
        # Sort deterministically
        vertices_with_sort_keys.sort(key=lambda x: x[0])
        
        # Create OrderedTransaction objects
        ordered = []
        for order_index, (_, vertex, tx_hash) in enumerate(vertices_with_sort_keys):
            ordered_tx = OrderedTransaction(
                tx_hash=tx_hash,
                tx_data=vertex.tx_data,
                round=vertex.round_number if vertex.round_number else self.current_round,
                creator_id=vertex.creator_id,
                order_index=order_index,
                finalized=vertex.round_number in self.finalized_rounds
                    if vertex.round_number else False
            )
            ordered.append(ordered_tx)
        
        self.ordered_transactions = ordered
        logger.info(f"Deterministically ordered {len(ordered)} transactions")
        
        return ordered
    
    def assign_round_numbers(self):
        """Assign round numbers to DAG vertices based on causal ordering."""
        all_vertices = self.dag.get_all_vertices()
        vertex_to_round: Dict[str, int] = {}
        
        # Vertices with no parents start at round 0
        for vertex in all_vertices:
            vertex_hash = vertex.compute_hash()
            
            if not vertex.parents:
                vertex_to_round[vertex_hash] = 0
            else:
                # Round = 1 + max(parent rounds)
                parent_rounds = [
                    vertex_to_round.get(p, 0) for p in vertex.parents
                ]
                vertex_to_round[vertex_hash] = max(parent_rounds) + 1 if parent_rounds else 0
            
            vertex.round_number = vertex_to_round[vertex_hash]
        
        logger.debug(f"Assigned round numbers to {len(vertex_to_round)} vertices")
    
    def get_finalized_transactions(self) -> List[OrderedTransaction]:
        """Get all finalized transactions."""
        return [tx for tx in self.ordered_transactions if tx.finalized]
    
    def get_transactions_by_round(self, round_number: int) -> List[OrderedTransaction]:
        """Get all transactions in a specific round."""
        return [tx for tx in self.ordered_transactions if tx.round == round_number]
    
    def get_round_status(self, round_number: int) -> Dict:
        """Get status of a round."""
        has_qc = round_number in self.qc_chain
        is_finalized = round_number in self.finalized_rounds
        tx_count = len(self.get_transactions_by_round(round_number))
        
        return {
            "round": round_number,
            "has_qc": has_qc,
            "is_finalized": is_finalized,
            "transaction_count": tx_count,
            "qc_signers": len(self.qc_chain[round_number].signers) if has_qc else 0
        }
    
    def check_round_timeout(self) -> bool:
        """Check if current round has timed out.
        
        Returns:
            True if timeout occurred
        """
        elapsed = time.time() - self.last_round_advance
        if elapsed > self.round_timeout:
            logger.warning(f"Round {self.current_round} timed out after {elapsed:.2f}s")
            self.advance_round()
            return True
        return False
    
    def get_statistics(self) -> dict:
        """Get consensus statistics."""
        finalized_count = len(self.get_finalized_transactions())
        
        return {
            "current_round": self.current_round,
            "total_rounds_with_qc": len(self.qc_chain),
            "finalized_rounds": len(self.finalized_rounds),
            "total_ordered_transactions": len(self.ordered_transactions),
            "finalized_transactions": finalized_count,
            "time_since_round_advance": time.time() - self.last_round_advance
        }