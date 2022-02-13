package txmanager

type Transaction struct {
	Id    string
	Ops   []Operation
	Abort bool
}

func (tx *Transaction) AddOps(ops ...Operation) {
	tx.Ops = append(tx.Ops, ops...)
}
