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
pub struct Queue {
    pub buffer: [i64; QUEUE_SIZE],
    pub shadow_head: AtomicUsize,
    pub head: AtomicUsize,
    pub shadow_tail: AtomicUsize, //writers pointer
    pub tail: AtomicUsize, //pointer of readable elements
    pub next_tx: AtomicI64,
    pub last_commited_tx: AtomicI64,
    pub pending_transactions: [i64; (QUEUE_SIZE/ WRITE_SLICE_S) + 1],
    pub expected_slice_os: AtomicI64,
    pub next_slice_id: AtomicI64,
    pub last_slice_id: AtomicI64,
    pub pending_slices: [i64; NUM_ITEMS + 2], //TODO: set correct size
    pub epoch: AtomicI64,

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
    pub slice_id: i64,
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
        if self.slice_id == -1 {
            return;
        }
        self.queue.pending_slices[self.slice_id as usize] *= -1;
        loop {
            let last_s_id = self.queue.last_slice_id.load(Ordering::SeqCst);
            let mut cond = last_s_id + 1 == self.slice_id;
//            println!("last s id {}, curr s id {}", last_s_id, self.slice_id);

            if cond {
                let mut max_tx_id = self.slice_id as usize;
                let mut sum = 0;
                while self.queue.pending_slices[max_tx_id] > 0 {
                    sum += self.queue.pending_slices[max_tx_id];
                    max_tx_id += 1;
                }
                // the actual max_tx_id is the previous one
                max_tx_id -= 1;

                cond = self.queue.last_slice_id.compare_exchange(last_s_id, max_tx_id as i64, Ordering::SeqCst, Ordering::SeqCst).is_ok();
                if cond {
                    // If the next expected slice was commited after the above while loop
                    // was finished, we need to re-enter the loop and commit it 
                    if self.queue.pending_slices[max_tx_id + 1] > 0 {
                        cond = self.queue.last_slice_id.compare_exchange(max_tx_id as i64, self.slice_id as i64 - 1, Ordering::SeqCst, Ordering::SeqCst).is_ok();
                        if cond {
                            continue;
                        }
                    }
                    let mut prev_head;
                    loop {
                        prev_head = self.queue.head.load(Ordering::SeqCst);
                        if self.queue.head.compare_exchange(prev_head, (prev_head + sum as usize) % self.queue.buffer.len(), Ordering::SeqCst, Ordering::SeqCst).is_ok() {
//                            println!("Sum {}", sum);
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
//            let slice_id = self.slice_id;
//            if self.queue.last_slice_id.compare_exchange(slice_id,
//                                                         slice_id + 1,
//                                                         Ordering::SeqCst,
//                                                         Ordering::SeqCst).is_ok() {
//                loop {
//                    let head = self.queue.head.load(Ordering::SeqCst);
//                    if self.queue.head.compare_exchange(head, (head + self.len) % self.queue.buffer.len(), Ordering::SeqCst, Ordering::SeqCst).is_ok() {
//                        break;
//                    }
//                }
//                println!("slice {}", self.slice_id);
//                break;
//            }
//            if self.queue.head.compare_exchange(head, (head + self.len) % self.queue.buffer.len(), Ordering::SeqCst, Ordering::SeqCst).is_ok() {

//    fn size(&self) -> usize {
//        return self.len;
//    }

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
        //println!("Value : {}", v);
    }

    pub unsafe fn commit(&mut self)  {
        let ptr = self.queue.get();
        (*ptr).commit_tx(self.tx_id);
    } 
}


impl Default for Queue {
    fn default() -> Queue {
        Queue {
            buffer: [0; QUEUE_SIZE],
            shadow_head: AtomicUsize::new(0),
            head: AtomicUsize::new(0),
            shadow_tail: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            next_tx: AtomicI64::new(0),
            last_commited_tx: AtomicI64::new(-1),
            expected_slice_os: AtomicI64::new(0),
            pending_transactions: [0; (QUEUE_SIZE/ WRITE_SLICE_S) + 1],
            next_slice_id: AtomicI64::new(0),
            last_slice_id: AtomicI64::new(-1),
            pending_slices: [0; NUM_ITEMS + 2],
            w_ind: 0,
            r_ind: 0,
            sum: AtomicI64::new(0),
            epoch: AtomicI64::new(1),
        }
    }
}

impl Queue {
    fn commit_tx(&mut self, tx_id: usize) {
        // commit the tx (do not finalize with 0 yet)
        self.pending_transactions[tx_id] *= -1;

        loop {
            let last_tx = self.last_commited_tx.load(Ordering::SeqCst);
            let mut cond = (last_tx + 1) % SLICES_FIT != tx_id as i64;
//            println!("tx_id  {}, expected tx_id {}, tail {}, shadow tail {}, head {}, shadow head {}", tx_id, (self.last_commited_tx.load(Ordering::SeqCst) + 1) % 50, self.tail.load(Ordering::SeqCst), self.shadow_tail.load(Ordering::SeqCst), self.head.load(Ordering::SeqCst), self.shadow_head.load(Ordering::SeqCst));
            // breaking.
            // if we enter this condition, this tx is immediately after last commited tx
            if !cond {
//                println!("tx_id  {}, tail {}", tx_id, self.tail.load(Ordering::SeqCst));
                let mut max_tx_id = tx_id;
                let mut sum = 0;
                // What if the whole buffer contains committed transactions?
                // infinite loop?
                while self.pending_transactions[max_tx_id] > 0 {
                    println!("tx {}, curr_max_tx {}", tx_id, max_tx_id);
                    sum += self.pending_transactions[max_tx_id];
                    max_tx_id = (max_tx_id + 1) % SLICES_FIT as usize;
                }
                println!("vghka");
                // the actual max_tx_id is the previous one
                max_tx_id = (max_tx_id as i64 - 1).rem_euclid(QUEUE_SIZE as i64/ WRITE_SLICE_S as i64) as usize;
//                println!("{}, next : {}", max_tx_id, (max_tx_id + 1) % SLICES_FIT as usize);
                let mut i = tx_id;
                let mut temp_store = [0; NUM_ITEMS + 1];
                while i != max_tx_id {
                    temp_store[i] = self.pending_transactions[i];
                    self.pending_transactions[i] = 0;
                    i = (i + 1) % SLICES_FIT as usize;
                }
                temp_store[max_tx_id] = self.pending_transactions[max_tx_id];
                self.pending_transactions[max_tx_id] = 0;
                cond = self.last_commited_tx.compare_exchange(last_tx, max_tx_id as i64, Ordering::SeqCst, Ordering::SeqCst).is_ok();
                if cond {
                    // If the next expected transaction was commited after the above while loop
                    // was finished, we need to re-enter the loop and commit it 
                    if self.pending_transactions[(max_tx_id + 1) % SLICES_FIT as usize] > 0 {
                        cond = self.last_commited_tx.compare_exchange(max_tx_id as i64, tx_id as i64 - 1, Ordering::SeqCst, Ordering::SeqCst).is_ok();
                        if cond {
                            let mut i = tx_id;
                            while i != max_tx_id {
                                self.pending_transactions[i] = temp_store[i];
                                i = (i + 1) % SLICES_FIT as usize;
                            }
                            self.pending_transactions[max_tx_id] = temp_store[max_tx_id];
                            continue;
                        }
                    }
//                    let prev_sum = self.sum.swap(self.sum.load(Ordering::SeqCst) + sum, Ordering::SeqCst);
                    let mut prev_tail;
                    loop {
                        prev_tail = self.tail.load(Ordering::SeqCst);
                        if self.tail.compare_exchange(prev_tail, (prev_tail + sum as usize) % self.buffer.len(), Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                            break;
                        }
                    }
//                    println!("tx {}, new max_tx_id {}, p_tail {}, n_tail {}, shadow tail {}, sum {}, total_sum {}, prev_sum {}, head {}, shadow head {}", tx_id, max_tx_id, prev_tail, (prev_tail + sum as usize) % self.buffer.len(), self.shadow_tail.load(Ordering::SeqCst), sum, self.sum.load(Ordering::SeqCst), prev_sum, self.head.load(Ordering::SeqCst), self.shadow_head.load(Ordering::SeqCst));
//                    println!("tx {} Commited, new tail {}, shadow tail {}, sum {}", tx_id, self.tail.load(Ordering::SeqCst), self.shadow_tail.load(Ordering::SeqCst), sum);
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

    pub fn reserve(&mut self, count: usize) -> Option<WritableSlice> {
        // Check if there is enough space in the queue for the 
        // reservation count request
        let mut cur : usize;

        let mut tx_id : i64;
        loop {
            cur = self.shadow_tail.load(Ordering::SeqCst);
            if self.free_space() < count {
                return None; 
            }
            if self.shadow_tail.compare_exchange(cur, (cur + count) % self.buffer.len(), Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                break;
            }
        }
        // TODO: with current implementation, later transactions can get lower tx_ids than previous ones
//        loop {
//            tx_id = self.next_tx.load(Ordering::SeqCst);
//            if self.next_tx.compare_exchange(tx_id, (tx_id + 1) % THREADS, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
//                break;
//            }        
//        }
        tx_id = (cur / WRITE_SLICE_S) as i64;
        assert!(self.pending_transactions[tx_id as usize] == 0);
        self.pending_transactions[tx_id as usize] = -(count as i64);
        return Some(WritableSlice::new(self, cur, 0, tx_id as usize, count));
    }

    pub fn free_space(&self) -> usize {
        let head = self.head.load(Ordering::SeqCst);
        let s_tail = self.shadow_tail.load(Ordering::SeqCst);
        // after first time head will never become same as tail again
        let ret = if head <= s_tail { 
            self.buffer.len() - s_tail + head
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
            self.buffer.len() - head + tail
        };

        return ret as usize;
    }

    pub fn dequeue_multiple(&mut self, count: i64) -> Slice {
        let mut cur : usize;
        let mut len : usize;
        loop {
            cur = self.shadow_head.load(Ordering::SeqCst);
            let cur_tail = self.tail.load(Ordering::SeqCst);
            let free_space = self.free_space();
            let readable_amount = self.readable_amount();
            let occupied_space = QUEUE_SIZE - free_space;
            len = cmp::min(readable_amount, count as usize);
//            if len == 10 {
//            }
            if self.shadow_head.compare_exchange(cur, (cur + len) % self.buffer.len(), Ordering::SeqCst, Ordering::SeqCst).is_ok() {
//                println!("Occupied space {}, free space {}, head {}, s_head {}, s_tail {}, readable amount {}, tail {}", occupied_space, free_space, 
//                         self.head.load(Ordering::SeqCst), cur, self.shadow_tail.load(Ordering::SeqCst), readable_amount, self.tail.load(Ordering::SeqCst));
                break;
            }
        }
        let mut s_id = -1;
        // loop probably not needed, check
        if len > 0 {
            loop {
                if cur == self.expected_slice_os.load(Ordering::SeqCst) as usize {
                    s_id = self.next_slice_id.fetch_add(1, Ordering::SeqCst);
                    self.expected_slice_os.store(((cur + len) % self.buffer.len()) as i64, Ordering::SeqCst);
                    break;
                }
                else {
//                    println!("NONO");
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
