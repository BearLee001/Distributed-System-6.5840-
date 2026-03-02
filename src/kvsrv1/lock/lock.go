package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	id string
	v  rpc.Tversion

	who string // ?
}

// MakeLock The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck, id: lockname, v: 0, who: kvtest.RandValue(8)}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	for {
		state, v, err := lk.ck.Get(lk.id)
		if err == rpc.OK && state == lk.who {
			lk.updateV(v)
			return
		}
		if err == rpc.OK && state == rpc.LOCK_IDLE || err == rpc.ErrNoKey { /* idle or never-locked */
			if !(err == rpc.ErrNoKey) {
				lk.updateV(v)
			}
			/* If another process performs a Put first, the server's version will be updated and it will return ErrVersion */
			replyErr := lk.ck.Put(lk.id, lk.who, lk.v)
			if replyErr == rpc.OK {
				//fmt.Println(lk.id + "got the lock!!!")
				lk.increment()
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	//fmt.Println(lk.id + " try to release the lock!!!")
	for {
		who, v, err := lk.ck.Get(lk.id)
		if err == rpc.OK {
			if who == lk.who && v == lk.v {
				replyErr := lk.ck.Put(lk.id, rpc.LOCK_IDLE, lk.v)
				if replyErr == rpc.OK {
					lk.increment()
					return
				}
			} else {
				// Unreachable. Why would the process release a lock that it doesn't own?
				return
			}
		}
	}
}

func (lk *Lock) updateV(newV rpc.Tversion) {
	lk.v = newV
}

func (lk *Lock) increment() {
	lk.v++
}
