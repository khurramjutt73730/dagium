package transaction

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"errors"
)

type Hash [32]byte
type Address [32]byte

type Transaction struct {
	TxID            Hash
	Sender          Address
	Receiver        Address
	Amount          uint64
	Nonce           uint64
	ParentTxIDs     []Hash
	Timestamp       uint64
	Expiry          uint64
	ChainID         uint32
	SignatureDomain []byte
	Signature       []byte
}

func (tx *Transaction) ComputeHash() Hash {
	var buf bytes.Buffer

	buf.Write(tx.Sender[:])
	buf.Write(tx.Receiver[:])

	_ = binary.Write(&buf, binary.BigEndian, tx.Amount)
	_ = binary.Write(&buf, binary.BigEndian, tx.Nonce)

	for _, p := range tx.ParentTxIDs {
		buf.Write(p[:])
	}

	_ = binary.Write(&buf, binary.BigEndian, tx.Timestamp)
	_ = binary.Write(&buf, binary.BigEndian, tx.Expiry)
	_ = binary.Write(&buf, binary.BigEndian, tx.ChainID)

	buf.Write(tx.SignatureDomain)

	return sha256.Sum256(buf.Bytes())
}

func (tx *Transaction) Sign(privateKey ed25519.PrivateKey) {
	hash := tx.ComputeHash()
	tx.Signature = ed25519.Sign(privateKey, hash[:])
	tx.TxID = hash
}

func (tx *Transaction) VerifySignature() bool {
	hash := tx.ComputeHash()
	return ed25519.Verify(
		ed25519.PublicKey(tx.Sender[:]),
		hash[:],
		tx.Signature,
	)
}

func (tx *Transaction) BasicValidate(currentTime uint64, expectedChainID uint32) error {

	if tx.ChainID != expectedChainID {
		return errors.New("invalid chain id")
	}

	if tx.Expiry < currentTime {
		return errors.New("transaction expired")
	}

	if len(tx.ParentTxIDs) < 2 || len(tx.ParentTxIDs) > 6 {
		return errors.New("invalid parent count")
	}

	if !tx.VerifySignature() {
		return errors.New("invalid signature")
	}

	return nil
}