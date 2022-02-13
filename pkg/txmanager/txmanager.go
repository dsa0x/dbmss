package txmanager

import "time"

type TxManager struct {
	txs map[string]*Transaction
}

type OpType string

var (
	Read  OpType = "read"
	Write OpType = "write"
)

type Operation struct {
	Duration time.Duration
	OpType   OpType
	Key      string
	Val      int
}
