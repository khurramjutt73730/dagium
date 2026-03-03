package dag

import (
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/khurramjutt73730/dagium/internal/transaction"
)

func createTx(pub ed25519.PublicKey, priv ed25519.PrivateKey, parents []transaction.Hash) *transaction.Transaction {

	var sender transaction.Address
	copy(sender[:], pub)

	tx := &transaction.Transaction{
		Sender:          sender,
		Receiver:        sender,
		Amount:          1,
		Nonce:           1,
		ParentTxIDs:     parents,
		Timestamp:       uint64(time.Now().Unix()),
		Expiry:          uint64(time.Now().Add(time.Minute).Unix()),
		ChainID:         1,
		SignatureDomain: []byte("DAGIUM-V0"),
	}

	tx.Sign(priv)
	return tx
}

func TestDAGInsert(t *testing.T) {

	pub, priv, _ := ed25519.GenerateKey(nil)
	d := NewDAG(50)

	// Create 2 genesis transactions
	tx1 := createTx(pub, priv, []transaction.Hash{{}, {}})
	tx2 := createTx(pub, priv, []transaction.Hash{{}, {}})

	d.AddGenesis(tx1)
	d.AddGenesis(tx2)

	tx3 := createTx(pub, priv, []transaction.Hash{tx1.TxID, tx2.TxID})

	err := d.AddTransaction(tx3)
	if err != nil {
		t.Fatal(err)
	}
}
