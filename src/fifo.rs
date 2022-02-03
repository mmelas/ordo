use std::sync::atomic::{AtomicUsize, AtomicI64, Ordering};
use std::cmp;
use std::cell::UnsafeCell;

// temporarily global variable
// NUM_ITEMS must be multiple of 8
const NUM_ITEMS : usize = 9_000;
const THREADS : i64 = 40_000;

/*
 * Ring buffer
 */
pub struct Queue {
    pub buffer: [i64; NUM_ITEMS + 1],
    pub shadow_head: AtomicUsize,
    pub head: AtomicUsize,
    pub shadow_tail: AtomicUsize, //writers pointer
    pub tail: AtomicUsize, //pointer of readable elements
    pub next_tx: AtomicI64,
    pub last_commited_tx: AtomicI64,
    pub pending_transactions: [i64; NUM_ITEMS + 1],

    /*
     * Needed for non-slices impl (baseline test) only
     */
    pub w_ind: usize,
    pub r_ind: usize,
    pub sum: AtomicI64,
}

unsafe impl Send for Queue {}
unsafe impl Sync for Queue {}

/*
 * Local data structure for each consumer in order 
 * to obtain a slice of the queue and dequeue without 
 * having to keep locking the other consumers
 */
pub struct Slice<'a> {
    pub queue: &'a mut Queue, // Revise lifetime params
    pub offset: usize,
    pub len: usize,
}

// UnsafeCell probably not needed. Check
pub struct WritableSlice<'a> {
    queue: &'a UnsafeCell<Queue>,
    offset: usize,
    curr_i: usize,
    tx_id: usize,
    pub len: usize,
}

// Probably not needed. Check (going together with UnsafeCell above)
unsafe impl<'a> Send for WritableSlice<'a> {}
unsafe impl<'a> Sync for WritableSlice<'a> {}

impl<'a> Slice<'a> {
    pub fn commit(&mut self) {
        let mut head;
        loop {
            head = self.queue.head.load(Ordering::SeqCst);
            if self.queue.head.compare_exchange(head, (head + self.len) % self.queue.buffer.len(), Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                break;
            }
        }
    }

//    fn size(&self) -> usize {
//        return self.len;
//    }
}

impl<'a> WritableSlice<'a> {
    pub fn new(slice: &'a mut Queue, os: usize, ci: usize, tid: usize, length: usize) -> Self {
        let ptr = slice as *mut Queue as *const UnsafeCell<Queue>;
        Self {
            queue: unsafe { &*ptr },
            offset: os,
            curr_i: ci,
            tx_id: tid,
            len: length,
        }
    }
    pub unsafe fn update(&mut self, v: i64) {
        let ptr = self.queue.get();
        let ind = (self.offset + self.curr_i) % (*ptr).buffer.len();
        (*ptr).buffer[ind] = v;
        self.curr_i += 1;
    }

    pub unsafe fn commit(&mut self)  {
        let ptr = self.queue.get();
        (*ptr).commit_tx(self.tx_id);
    } 
}


impl Default for Queue {
    fn default() -> Queue {
        Queue {
            buffer: [0; NUM_ITEMS + 1],
            shadow_head: AtomicUsize::new(0),
            head: AtomicUsize::new(0),
            shadow_tail: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            next_tx: AtomicI64::new(0),
            last_commited_tx: AtomicI64::new(-1),
            pending_transactions: [0; NUM_ITEMS + 1],
            w_ind: 0,
            r_ind: 0,
            sum: AtomicI64::new(0),
        }
    }
}

impl Queue {
    fn commit_tx(&mut self, tx_id: usize) {
//        self.tail.store((self.tail.load(Ordering::SeqCst) + 10) % self.buffer.len(), Ordering::SeqCst);
        // commit the tx (do not finalize with 0 yet)
//        self.pending_transactions[tx_id] *= -1;

        self.pending_transactions[tx_id] *= -1;
        loop {
            let last_tx = self.last_commited_tx.load(Ordering::SeqCst);
            let mut cond = (last_tx + 1) % THREADS != tx_id as i64;
//            println!("tx_id  {}, expected tx_id {}, tail {}", tx_id, self.last_commited_tx.load(Ordering::SeqCst) + 1, self.tail.load(Ordering::SeqCst));
            // breaking.
            // if we enter this condition, this tx is immediately after last commited tx
//            if (self.last_commited_tx.load(Ordering::SeqCst) + 1) == tx_id as i64 {
            if !cond {
//                println!("tx_id  {}, tail {}", tx_id, self.tail.load(Ordering::SeqCst));
                let mut max_tx_id = tx_id;
                let mut sum = 0;
                // What if the whole buffer contains committed transactions?
                // infinite loop?
                while self.pending_transactions[max_tx_id] > 0 {
                    sum += self.pending_transactions[max_tx_id];
                    max_tx_id = (max_tx_id + 1) % THREADS as usize;
                }
                // the actual max_tx_id is the previous one
                max_tx_id = (max_tx_id as i64 - 1).rem_euclid(THREADS) as usize;

                // commit the pending transactions and advance the write pointer
                // TODO: do we need compare exchange and condition? last_commited_tx
                // cannot change from a different thread because only one thread can 
                // enter this current condition. If that's the case, can't we 
                // include this on the above while loop?
                cond = self.last_commited_tx.compare_exchange(last_tx, max_tx_id as i64, Ordering::SeqCst, Ordering::SeqCst).is_ok();
                if cond {
                    // If the next expected transaction was commited after the above while loop
                    // was finished, we need to re-enter the loop and commit it 
                    if self.pending_transactions[max_tx_id + 1] > 0 {
                        cond = self.last_commited_tx.compare_exchange(max_tx_id as i64, tx_id as i64 - 1, Ordering::SeqCst, Ordering::SeqCst).is_ok();
                        if cond {
                            continue;
                        }
                    }
                    let prev_sum = self.sum.swap(self.sum.load(Ordering::SeqCst) + sum, Ordering::SeqCst);
                    let mut prev_tail;
                    loop {
                        prev_tail = self.tail.load(Ordering::SeqCst);
                        if self.tail.compare_exchange(prev_tail, (prev_tail + sum as usize) % self.buffer.len(), Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                            break;
                        }
                    }
//                    println!("Commited, new tail {}, sum {}", self.tail.load(Ordering::SeqCst) + sum as usize, sum);
//                    println!("tx {}, new max_tx_id {}, p_tail {}, n_tail {}, sum {}, total_sum {}, prev_sum {}", tx_id, max_tx_id, prev_tail, prev_tail + sum as usize, sum, self.sum.load(Ordering::SeqCst), prev_sum);
                    let mut i = tx_id;
                    while i != max_tx_id {
                        self.pending_transactions[i] = 0;
                        i = (i + 1) % THREADS as usize;
                    }
                    self.pending_transactions[max_tx_id] = 0;
                    break;
                }
                else {
//                    println!("DEBUG last tx {}, last_commited_tx {}", last_tx, self.last_commited_tx.load(Ordering::SeqCst));
                }
            }
            if cond {
                break;
            }
        }
    }

    pub fn reserve(&mut self, count: usize) -> Option<WritableSlice> {
        // Check if there is enough space in the queue for the 
        // reservation count request
        let mut cur : usize;

        let mut tx_id : i64;
        // always leave 1 space empty
        // between head and tail in order
        // to distinguish empty from full buffer
        if self.free_space() >= count {
            loop {
                cur = self.shadow_tail.load(Ordering::SeqCst);
                if self.shadow_tail.compare_exchange(cur, (cur + count) % self.buffer.len(), Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                    break;
                }
            }
            loop {
                tx_id = self.next_tx.load(Ordering::SeqCst);
                if self.next_tx.compare_exchange(tx_id, (tx_id + 1) % THREADS, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                    break;
                }        
            }
            assert!(self.pending_transactions[tx_id as usize] == 0);
            self.pending_transactions[tx_id as usize] = -(count as i64);
            return Some(WritableSlice::new(self, cur, 0, tx_id as usize, count));
        } else {
            return None;
        } 
    }

    pub fn free_space(&self) -> usize {
        let head = self.head.load(Ordering::SeqCst);
        let tail = self.tail.load(Ordering::SeqCst);
        let s_tail = self.shadow_tail.load(Ordering::SeqCst);
        // after first time head will never become same as tail again
        let ret = if head <= s_tail { 
            self.buffer.len() - s_tail + head
        } else {
            head - s_tail
        }; 
        // 1 slot is not used        
        return ret - 1 as usize;
    }

    pub fn dequeue_multiple(&mut self, count: i64) -> Slice {
        let mut cur : usize;
        let mut len : usize;
        loop {
            cur = self.shadow_head.load(Ordering::SeqCst);
            let free_space = self.free_space();
            let occupied_space = NUM_ITEMS - free_space;
            len = cmp::min(occupied_space, count as usize);
//            if len == 10 {
                println!("Occupied space {}, free space {}, head {}, s_tail {}", occupied_space, free_space, 
                         self.head.load(Ordering::SeqCst), self.shadow_tail.load(Ordering::SeqCst));
//            }
            if self.shadow_head.compare_exchange(cur, (cur + len) % self.buffer.len(), Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                break;
            }
        }
        return Slice{queue: self, offset: cur, len: len};
    }

}
