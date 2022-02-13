package txmanager

import (
	"sync"
	"time"
)

func Main() {
	main4()
	return

	wg := sync.WaitGroup{}

	txn1 := Transaction{
		Id: "1",
	}

	txn1.AddOps(
		Operation{
			OpType: Read,
			Key:    "A",
		},
		Operation{
			OpType: Write,
			Key:    "A",
			Val:    350,
		},
		Operation{
			OpType: Write,
			Key:    "B",
			Val:    1001,
		},
	)

	txn2 := Transaction{
		Id: "2",
	}

	txn2.AddOps(
		Operation{
			OpType: Read,
			Key:    "A",
		},
		// Operation{
		// 	OpType: Read,
		// 	Key:    "B",
		// },
		// Operation{
		// 	OpType: Write,
		// 	Key:    "B",
		// 	Val:    1200,
		// },
		// Operation{
		// 	OpType: Write,
		// 	Key:    "A",
		// 	Val:    500,
		// },
	)
	txn3 := Transaction{
		Id: "3",
	}

	txn3.AddOps(
		Operation{
			OpType: Write,
			Key:    "A",
			Val:    7000,
		},
		Operation{
			OpType: Write,
			Key:    "B",
			Val:    1500,
		},
	)

	tp := TwoPhase{}
	tp.Init()
	tp.AddTxn(txn1, txn2, txn3)

	for i := 0; i < tp.TxnLen(); i++ {
		wg.Add(1)
		go func(idx int) {
			txn := tp.GetTxn(idx)
			tp.StartTransaction2(txn)
			wg.Done()
		}(i)
		time.Sleep(time.Millisecond)
	}
	wg.Wait()
}

func main2() {
	wg := sync.WaitGroup{}

	txn1 := Transaction{
		Id: "1",
	}

	txn1.AddOps(
		Operation{
			OpType: Read,
			Key:    "A",
		},
		Operation{
			OpType: Write,
			Key:    "A",
			Val:    900,
		},
		Operation{
			OpType: Read,
			Key:    "B",
		},
		Operation{
			OpType: Write,
			Key:    "B",
			Val:    1100,
		},
	)

	txn2 := Transaction{
		Id: "2",
	}

	txn2.AddOps(
		Operation{
			OpType: Read,
			Key:    "A",
		},
		Operation{
			OpType: Read,
			Key:    "B",
		},
	)

	tp := TwoPhase{}
	tp.Init()
	tp.AddTxn(txn1, txn2)

	for i := 0; i < tp.TxnLen(); i++ {
		wg.Add(1)
		go func(idx int) {
			txn := tp.GetTxn(idx)
			tp.StartTransaction(txn)
			wg.Done()
		}(i)
		time.Sleep(time.Millisecond)
	}
	wg.Wait()
}

// when a txn aborts
// StartTransaction result will be inconsistent
// StartTransaction2 should be fine
func main3() {
	wg := sync.WaitGroup{}

	txn1 := Transaction{
		Id:    "1",
		Abort: true,
	}

	txn1.AddOps(
		Operation{
			OpType: Read,
			Key:    "A",
		},
		Operation{
			OpType: Write,
			Key:    "A",
			Val:    900,
		},
		Operation{
			OpType: Read,
			Key:    "B",
		},
		Operation{
			OpType: Write,
			Key:    "B",
			Val:    1100,
		},
	)

	txn2 := Transaction{
		Id: "2",
	}

	txn2.AddOps(
		Operation{
			OpType: Read,
			Key:    "A",
		},
		Operation{
			OpType: Read,
			Key:    "B",
		},
	)

	tp := TwoPhase{}
	tp.Init()
	tp.AddTxn(txn1, txn2)

	for i := 0; i < tp.TxnLen(); i++ {
		wg.Add(1)
		go func(idx int) {
			txn := tp.GetTxn(idx)
			tp.StartTransaction(txn)
			wg.Done()
		}(i)
		time.Sleep(time.Millisecond * 5)
	}
	wg.Wait()
}

func main4() {
	wg := sync.WaitGroup{}

	txn1 := Transaction{
		Id: "1",
	}

	txn1.AddOps(
		Operation{
			OpType: Read,
			Key:    "A",
		},
		Operation{
			OpType: Write,
			Key:    "A",
			Val:    900,
		},
		Operation{
			OpType: Read,
			Key:    "B",
		},
		Operation{
			OpType: Write,
			Key:    "B",
			Val:    1100,
		},
	)

	txn2 := Transaction{
		Id: "2",
	}

	txn2.AddOps(
		Operation{
			OpType: Read,
			Key:    "B",
		},
		Operation{
			OpType: Write,
			Key:    "B",
			Val:    1100,
		},
		Operation{
			OpType: Read,
			Key:    "A",
		},
	)

	tp := TwoPhase{}
	tp.Init()
	tp.AddTxn(txn1, txn2)

	for i := 0; i < tp.TxnLen(); i++ {
		wg.Add(1)
		go func(idx int) {
			txn := tp.GetTxn(idx)
			tp.StartTransaction(txn)
			wg.Done()
		}(i)
		// time.Sleep(time.Millisecond * 5)
	}
	wg.Wait()
}
