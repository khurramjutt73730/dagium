package dag

import (
	"errors"
	"sync"

	"github.com/khurramjutt73730/dagium/internal/transaction"
)

type Vertex struct {
	Tx        *transaction.Transaction
	Parents   []*Vertex
	Children  []*Vertex
	Depth     uint32
	Finalized bool
}

type DAG struct {
	mu       sync.RWMutex
	vertices map[transaction.Hash]*Vertex
	tips     map[transaction.Hash]*Vertex
	maxDepth uint32
}

func NewDAG(maxDepth uint32) *DAG {
	return &DAG{
		vertices: make(map[transaction.Hash]*Vertex),
		tips:     make(map[transaction.Hash]*Vertex),
		maxDepth: maxDepth,
	}
}

func (d *DAG) AddTransaction(tx *transaction.Transaction) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, exists := d.vertices[tx.TxID]; exists {
		return errors.New("duplicate transaction")
	}

	parentCount := len(tx.ParentTxIDs)
	if parentCount < 2 || parentCount > 6 {
		return errors.New("invalid parent count")
	}

	parents := make([]*Vertex, 0, parentCount)
	var maxParentDepth uint32

	for _, pid := range tx.ParentTxIDs {
		parent, exists := d.vertices[pid]
		if !exists {
			return errors.New("parent not found")
		}

		if parent.Depth > maxParentDepth {
			maxParentDepth = parent.Depth
		}

		parents = append(parents, parent)
	}

	newDepth := maxParentDepth + 1

	if newDepth > d.maxDepth {
		return errors.New("depth window exceeded")
	}

	vertex := &Vertex{
		Tx:      tx,
		Parents: parents,
		Depth:   newDepth,
	}

	for _, p := range parents {
		p.Children = append(p.Children, vertex)
		delete(d.tips, p.Tx.TxID)
	}

	d.vertices[tx.TxID] = vertex
	d.tips[tx.TxID] = vertex

	return nil
}

func (d *DAG) AddGenesis(tx *transaction.Transaction) {
	d.mu.Lock()
	defer d.mu.Unlock()

	vertex := &Vertex{
		Tx:    tx,
		Depth: 0,
	}

	d.vertices[tx.TxID] = vertex
	d.tips[tx.TxID] = vertex
}
