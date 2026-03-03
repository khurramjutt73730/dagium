package transaction

import (
	"crypto/ed25519"
	"testing"
	"time"
)

func TestTransactionSignAndVerify(t *testing.T) {

	pub, priv, _ := ed25519.GenerateKey(nil)

	var sender Address
	copy(sender[:], pub)

	tx := &Transaction{
		Sender:          sender,
		Receiver:        sender,
		Amount:          100,
		Nonce:           1,
		ParentTxIDs:     []Hash{{}, {}}, // 2 dummy parents
		Timestamp:       uint64(time.Now().Unix()),
		Expiry:          uint64(time.Now().Add(time.Minute).Unix()),
		ChainID:         1,
		SignatureDomain: []byte("DAGIUM-V0"),
	}

	tx.Sign(priv)

	if !tx.VerifySignature() {
		t.Fatal("signature verification failed")
	}

	err := tx.BasicValidate(uint64(time.Now().Unix()), 1)
	if err != nil {
		t.Fatal(err)
	}
}