use std::sync::atomic::{AtomicUsize, AtomicI64, Ordering};
use std::cmp;
use std::cell::UnsafeCell;
use crate::params;

const QUEUE_SIZE : usize = params::QUEUE_SIZE;
const WRITE_SLICE_S : usize = params::WRITE_SLICE_S;
const SLICES_FIT : i64 = QUEUE_SIZE as i64/ WRITE_SLICE_S as i64;
const NUM_ITEMS : usize = params::NUM_ITEMS;

/*
 * Ring buffer
 */
pub struct Queue<T:Default> {
    pub buffer: Box<[T]>,
    pub shadow_head: AtomicUsize,
    pub head: AtomicUsize,
    pub shadow_tail: AtomicUsize, //writers pointer
    pub tail: AtomicUsize, //pointer of readable elements
    pub next_tx: AtomicI64,
    pub last_commited_tx: AtomicI64,
    pub pending_transactions: [i64; 50_000],
//    pub pending_transactions: [i64; (NUM_ITEMS/ WRITE_SLICE_S) + 10],
    pub expected_slice_os: AtomicI64,
    pub next_rslice_id: AtomicI64,
    pub last_rslice_id: AtomicI64,
    pub pending_slices: [i64; 50_000], //TODO: set correct size
    pub epoch: AtomicI64,

    /*
     * Needed for non-slices impl (baseline test) only
     */
    pub w_ind: usize,
    pub r_ind: usize,
    pub sum: AtomicI64,
}

unsafe impl<T:Default> Send for Queue<T> {}
unsafe impl<T:Default> Sync for Queue<T> {}

/*
 * Local data structure for each consumer in order 
 * to obtain a slice of the queue and dequeue without 
 * having to keep locking the other consumers
 */
pub struct Slice<'a, T:Default> {
    pub queue: &'a mut Queue<T>, // Revise lifetime params
    pub offset: usize,
    pub slice_id: i64,
    pub len: usize,
}

// UnsafeCell probably not needed. Check
pub struct WritableSlice<'a, T:Default> {
    queue: &'a mut Queue<T>,
    offset: usize,
    curr_i: usize,
    tx_id: usize,
    pub len: usize,
}

// Probably not needed. Check (going together with UnsafeCell above)
//unsafe impl<'a> Send for WritableSlice<'a> {}
//unsafe impl<'a> Sync for WritableSlice<'a> {}

impl<'a, T:Default> Slice<'a, T> {
    pub fn commit(&mut self) {
        if self.slice_id == -1 {
            return;
        }
        self.queue.pending_slices[self.slice_id as usize] *= -1;
        loop {
            let last_s_id = self.queue.last_rslice_id.load(Ordering::SeqCst);
            let mut cond = last_s_id + 1 == self.slice_id;

            if cond {
                let mut max_tx_id = self.slice_id as usize;
                let mut sum = 0;
                while self.queue.pending_slices[max_tx_id] > 0 {
                    sum += self.queue.pending_slices[max_tx_id];
                    max_tx_id += 1;
                }
                // the actual max_tx_id is the previous one
                max_tx_id -= 1;

                cond = self.queue.last_rslice_id.compare_exchange(
                    last_s_id, max_tx_id as i64, 
                    Ordering::SeqCst, 
                    Ordering::SeqCst
                ).is_ok();
                if cond {
                    // If the next expected slice was commited after the above while loop
                    // was finished, we need to re-enter the loop and commit it 
                    if self.queue.pending_slices[max_tx_id + 1] > 0 {
                        cond = self.queue.last_rslice_id.compare_exchange(
                            max_tx_id as i64, self.slice_id as i64 - 1, 
                            Ordering::SeqCst, Ordering::SeqCst
                        ).is_ok();
                        if cond {
                            continue;
                        }
                    }
                    let mut prev_head;
                    loop {
                        prev_head = self.queue.head.load(Ordering::SeqCst);
                        if self.queue.head.compare_exchange(
                            prev_head, (prev_head + sum as usize) % self.queue.buffer.len(), 
                            Ordering::SeqCst, Ordering::SeqCst
                        ).is_ok() {
                            break;
                        }
                    }
//                    println!("tx {}, sum {}, head {}", self.slice_id, sum, prev_head + sum as usize);
                    break;
                }
            }
            else {
//                println!("Expected {}, got {}", last_s_id + 1, self.slice_id);
                break;
           }
        }
    }
}

impl<'a, T:Default> WritableSlice<'a, T> {
    pub fn new(slice: &'a mut Queue<T>, os: usize, ci: usize, tid: usize, length: usize) -> Self {
//        let ptr = slice as *mut Queue as *const UnsafeCell<Queue>;
        Self {
            queue: slice,
            offset: os,
            curr_i: ci,
            tx_id: tid,
            len: length,
        }
    }
    pub unsafe fn update(&mut self, v: T) {
//        let ptr = self.queue.get();
        let ind = (self.offset + self.curr_i) % self.queue.buffer.len();
//        (*ptr).buffer[ind] = v;
        self.queue.buffer[ind] = v;
        self.curr_i += 1;
        //println!("Value : {}", v);
    }

    pub unsafe fn commit(&mut self)  {
        self.queue.commit_tx(self.tx_id);
//        let ptr = self.queue.get();
//        (*ptr).commit_tx(self.tx_id);
    } 
}

impl<T:Default + Clone> Default for Queue<T> {
    fn default() -> Queue<T> {
        Queue {
            buffer: vec![T::default(); QUEUE_SIZE].into_boxed_slice(),
            shadow_head: AtomicUsize::new(0),
            head: AtomicUsize::new(0),
            shadow_tail: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            next_tx: AtomicI64::new(0),
            last_commited_tx: AtomicI64::new(-1),
            expected_slice_os: AtomicI64::new(0),
            pending_transactions: [0; 50_000],
//            pending_transactions: [0; (NUM_ITEMS/ WRITE_SLICE_S) + 10],
            next_rslice_id: AtomicI64::new(0),
            last_rslice_id: AtomicI64::new(-1),
            pending_slices: [0; 50_000],
            w_ind: 0,
            r_ind: 0,
            sum: AtomicI64::new(0),
            epoch: AtomicI64::new(0),
        }
    }
}

impl<T:Default> Queue<T> {
    fn commit_tx(&mut self, tx_id: usize) {
        // commit the tx (do not finalize with 0 yet)
        self.pending_transactions[tx_id] *= -1;
        loop {
            let last_tx = self.last_commited_tx.load(Ordering::SeqCst);
            let mut cond = (last_tx + 1) != tx_id as i64;
//            println!("tx_id  {}, expected tx_id {}, tail {}, shadow tail {}, head {}, shadow head {}", tx_id, last_tx + 1, self.tail.load(Ordering::SeqCst), self.shadow_tail.load(Ordering::SeqCst), self.head.load(Ordering::SeqCst), self.shadow_head.load(Ordering::SeqCst));
            // breaking.
            // if we enter this condition, this tx is immediately after last commited tx
            if !cond {
//                println!("tx_id  {}, tail {}", tx_id, self.tail.load(Ordering::SeqCst));
                let mut max_tx_id = tx_id;
                let mut sum = 0;
                while self.pending_transactions[max_tx_id] > 0 {
//                    println!("tx {}, curr_max_tx {}", tx_id, max_tx_id);
                    sum += self.pending_transactions[max_tx_id];
                    max_tx_id += 1;
                }
//                println!("vghka");
                // the actual max_tx_id is the previous one
                max_tx_id -= 1;
                cond = self.last_commited_tx.compare_exchange(
                    last_tx, max_tx_id as i64, 
                    Ordering::SeqCst, Ordering::SeqCst
                ).is_ok();
                if cond {
                    // If the next expected transaction was commited after the above while loop
                    // was finished, we need to re-enter the loop and commit it 
                    if self.pending_transactions[max_tx_id + 1] > 0 {
                        cond = self.last_commited_tx.compare_exchange(
                            max_tx_id as i64, tx_id as i64 - 1, 
                            Ordering::SeqCst, Ordering::SeqCst
                        ).is_ok();
                        if cond {
                            continue;
                        }
                    }
//                    let prev_sum = self.sum.swap(self.sum.load(Ordering::SeqCst) + sum, Ordering::SeqCst);
                    let mut prev_tail;
                    loop {
                        prev_tail = self.tail.load(Ordering::SeqCst);
                        if self.tail.compare_exchange(
                            prev_tail, (prev_tail + sum as usize) % QUEUE_SIZE, 
                            Ordering::SeqCst, Ordering::SeqCst
                        ).is_ok() {
                            break;
                        }
                    }
//                    println!("tx {}, new max_tx_id {}, p_tail {}, n_tail {}, shadow tail {}, sum {}, total_sum {}, prev_sum {}, head {}, shadow head {}", tx_id, max_tx_id, prev_tail, (prev_tail + sum as usize) % QUEUE_SIZE, self.shadow_tail.load(Ordering::SeqCst), sum, self.sum.load(Ordering::SeqCst), prev_sum, self.head.load(Ordering::SeqCst), self.shadow_head.load(Ordering::SeqCst));
//                    println!("tx {} Commited, new max_tx_id {}, new tail {}, shadow tail {}, sum {}", tx_id, max_tx_id, self.tail.load(Ordering::SeqCst), self.shadow_tail.load(Ordering::SeqCst), sum);
                   break;
                }
                else {
//                    println!("DEBUG last tx {}, last_commited_tx {}", last_tx, self.last_commited_tx.load(Ordering::SeqCst));
                }
            }
            else {
//                println!("Expected tx : {}, curr tx {}", last_tx + 1, tx_id);
                break;
            }
        }
    }

    pub fn reserve(&mut self, count: usize) -> Option<WritableSlice<T>> {
        // Check if there is enough space in the queue for the 
        // reservation count request
        let mut cur : usize;

        if count == 0 { return None }
        let mut tx_id : i64;
        loop {
            cur = self.shadow_tail.load(Ordering::SeqCst);
            if self.free_space() < count {
                return None; 
            }
//            loop {
//                tx_id = self.next_tx.load(Ordering::SeqCst);
//                if self.next_tx.compare_exchange(tx_id, tx_id + 1, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
//                    break;
//                }        
//            }
            if self.shadow_tail.compare_exchange(
                cur, (cur + count) /*% QUEUE_SIZE*/, 
                Ordering::SeqCst, Ordering::SeqCst
            ).is_ok() {
                break;
            }
        }
        // TODO: with current implementation, maybe later transactions can get lower tx_ids than previous ones? Otherwise do the implementation with next_expected_os like in Slices (fn dequeue_muliple)
        loop {
            tx_id = self.next_tx.load(Ordering::SeqCst);
            if self.next_tx.compare_exchange(
                tx_id, tx_id + 1, 
                Ordering::SeqCst, Ordering::SeqCst
            ).is_ok() {
                break;
            }        
        }
//        println!("cur {}, tx_id {}, count {}, struct address {:p}", cur, tx_id, count, &self);
//        tx_id = (cur / WRITE_SLICE_S) as i64;
//        if tx_id < 2019 {
//            println!("cur {}, tx id {}", cur, tx_id);
//        }
//        assert!(self.pending_transactions[tx_id as usize] == 0);
        self.pending_transactions[tx_id as usize] = -(count as i64);
        return Some(WritableSlice::new(self, cur, 0, tx_id as usize, count));
    }

    pub fn free_space(&self) -> usize {
        let head = self.head.load(Ordering::SeqCst);
        let s_tail = self.shadow_tail.load(Ordering::SeqCst) % QUEUE_SIZE;
        // after first time head will never become same as tail again
        let ret = if head <= s_tail { 
            QUEUE_SIZE - s_tail + head
        } else {
            head - s_tail
        }; 
        // 1 slot is not used (always leave 1 space empty
        // between head and tail in order to distinguish 
        // empty from full buffer)
        if ret <= 1 {
            return 0;
        }
        return ret - 1 as usize;
    }

    pub fn readable_amount(&self) -> usize {
        let head = self.shadow_head.load(Ordering::SeqCst);
        let tail = self.tail.load(Ordering::SeqCst);

        // If head == tail tail has reached head
        // so we can read the whole buffer
        // (but if this happens at start when head == tail problem?)
        let ret = if head <= tail {
            tail - head
        } else {
            QUEUE_SIZE - head + tail
        };

        return ret as usize;
    }

    pub fn dequeue_multiple(&mut self, count: i64) -> Slice<T> {
        let mut cur : usize;
        let mut len : usize;
        loop {
            cur = self.shadow_head.load(Ordering::SeqCst);
//            let cur_tail = self.tail.load(Ordering::SeqCst);
//            let free_space = self.free_space();
            let readable_amount = self.readable_amount();
//            let occupied_space = QUEUE_SIZE - free_space;
            len = cmp::min(readable_amount, count as usize);
//            if len == 10 {
//            }
            if self.shadow_head.compare_exchange(
                cur, (cur + len) % QUEUE_SIZE, 
                Ordering::SeqCst, Ordering::SeqCst
            ).is_ok() {
//                println!("Occupied space {}, free space {}, head {}, s_head {}, s_tail {}, readable amount {}, tail {}", occupied_space, free_space, 
//                         self.head.load(Ordering::SeqCst), cur, self.shadow_tail.load(Ordering::SeqCst) % QUEUE_SIZE, readable_amount, self.tail.load(Ordering::SeqCst));
                break;
            }
        }
        let mut s_id = -1;
        // loop probably not needed, check
        if len > 0 {
            loop {
                if cur == self.expected_slice_os.load(Ordering::SeqCst) as usize {
                    s_id = self.next_rslice_id.fetch_add(1, Ordering::SeqCst);
                    self.expected_slice_os.store(
                        ((cur + len) % QUEUE_SIZE) as i64, Ordering::SeqCst
                    );
                    break;
                }
                else {
//                    println!("Debug");
                }
            }
        }
//        println!("offset {}, s_id {}", cur, s_id);
        if s_id != -1 {
            self.pending_slices[s_id as usize] = -(len as i64);
        }
        return Slice{queue: self, offset: cur, slice_id: s_id, len: len};
    }

}
