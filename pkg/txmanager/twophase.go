package txmanager

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

type TwoPhase struct {
	op     string
	txns   []Transaction
	lm     LockManager
	locklm sync.Mutex //
	db     map[string]*Tuple
}

type Tuple struct {
	exlock sync.RWMutex
	shlock sync.RWMutex
	val    int
}

type LockManager struct {
	sync.Map
}

func (tp *TwoPhase) Init() {
	tp.db = make(map[string]*Tuple)
	tp.db["A"] = &Tuple{exlock: sync.RWMutex{}, shlock: sync.RWMutex{}, val: 2000}
	tp.db["B"] = &Tuple{exlock: sync.RWMutex{}, shlock: sync.RWMutex{}, val: 1000}

	tp.lm.Store("A", TupleLock{
		r: &sync.RWMutex{},
		w: &sync.Mutex{},
	})
	tp.lm.Store("B", TupleLock{
		r: &sync.RWMutex{},
		w: &sync.Mutex{},
	})
}

type TupleLock struct {
	r *sync.RWMutex
	w *sync.Mutex
}

type Lock struct {
	r map[string]*sync.RWMutex
	w map[string]*sync.Mutex
}

// StartTransaction two phase locking
func (tp *TwoPhase) StartTransaction(txn Transaction) {
	fmt.Printf("Starting transaction %s:\n", txn.Id)
	lockers := Lock{
		r: make(map[string]*sync.RWMutex),
		w: make(map[string]*sync.Mutex),
	}

	// growing phase
	// acquire all locks needed
	tp.locklm.Lock()
	for _, op := range txn.Ops {
		mu, ok := tp.lm.Load(op.Key)
		if !ok {
			panic("no key")
		}
		if op.OpType == Read {
			lockers.r[op.Key] = mu.(TupleLock).r
		} else {
			lockers.w[op.Key] = mu.(TupleLock).w
		}
	}
	// lock
	for _, lock := range lockers.r {
		lock.Lock()
	}
	for _, lock := range lockers.w {
		lock.Lock()
	}
	tp.locklm.Unlock()

	// to test abort
	tmp := map[string]int{}
	fin := map[string]int{}

	// shrinking phase
	// release locks after each op
	for _, op := range txn.Ops {
		tuple := tp.db[op.Key]
		time.Sleep(time.Millisecond * 10)
		if op.OpType == Read {
			fmt.Printf("Txn %s:Reading %s: %d\n", txn.Id, op.Key, tuple.val)
			lockers.r[op.Key].Unlock()
		} else if op.OpType == Write {
			tmp[op.Key] = tuple.val // used to revert on abort

			tuple.val = op.Val
			lockers.w[op.Key].Unlock()
			fmt.Printf("Txn %s:Writing %s: %d\n", txn.Id, op.Key, tuple.val)
		}
		fin[op.Key] = tuple.val

	}

	if txn.Abort {
		for k, v := range tmp {
			tuple := tp.db[k]
			tuple.val = v
		}
	}

	fmt.Printf("Txn %s: %v\n", txn.Id, fin)
	// fmt.Println()
	// tp.Snapshot(txn.Id)
}

// StartTransaction2 strict two phase locking
func (tp *TwoPhase) StartTransaction2(txn Transaction) {
	fmt.Printf("Starting transaction %s:\n", txn.Id)
	lockers := Lock{
		r: make(map[string]*sync.RWMutex),
		w: make(map[string]*sync.Mutex),
	}

	// growing phase
	// acquire all locks needed
	tp.locklm.Lock()
	for _, op := range txn.Ops {
		mu, ok := tp.lm.Load(op.Key)
		if !ok {
			panic("no key")
		}
		if op.OpType == Read {
			lockers.r[op.Key] = mu.(TupleLock).r
		} else {
			lockers.w[op.Key] = mu.(TupleLock).w
		}
	}
	// lock
	for _, lock := range lockers.r {
		lock.Lock()
	}
	for _, lock := range lockers.w {
		lock.Lock()
	}
	tp.locklm.Unlock()

	// to test abort
	tmp := map[string]int{}
	fin := map[string]int{}
	for _, op := range txn.Ops {
		tuple := tp.db[op.Key]
		if op.OpType == Write {
			tmp[op.Key] = tuple.val // used to revert on abort

			tuple.val = op.Val
		}
		fin[op.Key] = tuple.val
		fmt.Printf("Txns %s:%s %s: %d\n", txn.Id, op.OpType, op.Key, tuple.val)
	}

	if txn.Abort {
		for k, v := range tmp {
			tuple := tp.db[k]
			tuple.val = v
		}
	}

	// shrinking phase
	for _, lock := range lockers.r {
		lock.Unlock()
	}
	for _, lock := range lockers.w {
		lock.Unlock()
	}

	fmt.Printf("Txn %s: %v\n", txn.Id, fin)
	// copy(mp, tp.db)

	// tp.Snapshot(txn.Id)
}

func (tp *TwoPhase) TxnLen() int {
	return len(tp.txns)
}
func (tp *TwoPhase) GetTxn(idx int) Transaction {
	return tp.txns[idx]
}
func (tp *TwoPhase) AddTxn(txns ...Transaction) {
	tp.txns = append(tp.txns, txns...)
}
func (tp *TwoPhase) Snapshot(txnID string) {

	ls := []string{}
	for k := range tp.db {
		ls = append(ls, k)
	}
	sort.Strings(ls)

	str := "{"
	for _, k := range ls {
		str += fmt.Sprintf("%s: %d, ", k, tp.db[k].val)
	}
	str += "}\n"
	fmt.Printf("Txn %s: %s", txnID, str)
}
