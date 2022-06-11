use std::sync::atomic::{AtomicUsize, AtomicI64, Ordering};
use std::cmp;
use std::cell::UnsafeCell;
use crate::params;
use std::sync::atomic::compiler_fence;
use std::sync::atomic::fence;
use std::time::Instant;
use crate::metrics;

const QUEUE_SIZE : usize = params::QUEUE_SIZE;
const WRITE_SLICE_S : usize = params::WRITE_SLICE_S;
const SLICES_FIT : i64 = QUEUE_SIZE as i64/ WRITE_SLICE_S as i64;
const NUM_ITEMS : usize = params::NUM_ITEMS;

/*
 * Ring buffer
 */
pub struct Queue<T:Default> {
    pub buffer: Vec<T>,
    pub shadow_head: AtomicUsize,
    pub head: AtomicUsize,
    pub shadow_tail: AtomicUsize, //writers pointer
    pub tail: AtomicUsize, //pointer of readable elements
    pub next_tx: AtomicI64,
    pub last_commited_tx: AtomicI64,
    pub pending_transactions: Vec<i64>,
    pub expected_rslice_os: AtomicI64,
    pub expected_wslice_os: AtomicI64,
    pub next_rslice_id: AtomicI64,
    pub last_rslice_id: AtomicI64,
    pub pending_slices: Vec<i64>, //TODO: set correct size
    pub epoch: AtomicI64,
    pub fresh_val: Vec<bool>,
    pub r_ind: usize,
    pub w_ind: usize,
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
    pub queue: &'a mut Queue<T>,
    pub offset: usize,
    pub curr_i: usize,
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
//        unsafe{std::ptr::write_volatile(&mut self.queue.pending_slices[self.slice_id as usize], 
 //                                       std::ptr::read_volatile(&self.queue.pending_slices[self.slice_id as usize]) * -1)};
        self.queue.pending_slices[self.slice_id as usize] *= -1;
        fence(Ordering::SeqCst);
        loop {
            let last_s_id = self.queue.last_rslice_id.load(Ordering::SeqCst);
            let mut cond = (last_s_id + 1) % QUEUE_SIZE as i64 == self.slice_id;

            if cond {
                let mut max_tx_id = self.slice_id as i64;
                let mut sum = 0;
                while unsafe{std::ptr::read_volatile(&self.queue.pending_slices[max_tx_id as usize])} > 0 {
                    sum += unsafe{std::ptr::read_volatile(&self.queue.pending_slices[max_tx_id as usize])};
                    max_tx_id = (max_tx_id + 1) % QUEUE_SIZE as i64;
                }
                // the actual max_tx_id is the previous one
                max_tx_id = (max_tx_id - 1).rem_euclid(QUEUE_SIZE as i64);

                cond = self.queue.last_rslice_id.compare_exchange(
                    last_s_id, max_tx_id as i64, 
                    Ordering::SeqCst, 
                    Ordering::SeqCst
                ).is_ok();
                if cond {
                    // If the next expected slice was commited after the above while loop
                    // was finished, we need to re-enter the loop and commit it 
                    loop {
                        if unsafe{std::ptr::read_volatile(&self.queue.pending_slices[(self.slice_id - 1).rem_euclid(QUEUE_SIZE as i64) as usize])} == 0 {
                            break;
                        }
                    }
                    if self.queue.pending_slices[((max_tx_id + 1) % QUEUE_SIZE as i64) as usize] > 0 {
//                        println!("{}", self.slice_id as i64);
                        cond = self.queue.last_rslice_id.compare_exchange(
                            max_tx_id as i64, (self.slice_id as i64 - 1).rem_euclid(QUEUE_SIZE as i64),
                            Ordering::SeqCst, Ordering::SeqCst
                        ).is_ok();
                        if cond {
                            continue;
                        }
                    }
                    // if we reach this line, transactions got COMMITTED
                    let mut first_tx = self.slice_id as i64;
                    while first_tx != max_tx_id {
                        self.queue.pending_slices[first_tx as usize] = 0;
                        first_tx = (first_tx + 1) % QUEUE_SIZE as i64;
                    }
                    self.queue.pending_slices[max_tx_id as usize] = 0;
                    let mut prev_head;
                    loop {
                        prev_head = self.queue.head.load(Ordering::SeqCst);
                        if self.queue.head.compare_exchange(
                            prev_head, (prev_head + sum as usize) % QUEUE_SIZE, 
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
    // Maybe put an upper
    // bound index on write slices and return
    // error when trying to write to out of bounds index
    pub unsafe fn update(&mut self, v: T) {
        let ind = (self.offset + self.curr_i) % QUEUE_SIZE;
        self.queue.buffer[ind] = v;
        self.curr_i += 1;
        self.queue.fresh_val[ind] = true;
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
            buffer: vec![T::default(); QUEUE_SIZE],
            shadow_head: AtomicUsize::new(0),
            head: AtomicUsize::new(0),
            shadow_tail: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            next_tx: AtomicI64::new(0),
            last_commited_tx: AtomicI64::new(-1),
            expected_rslice_os: AtomicI64::new(0),
            expected_wslice_os: AtomicI64::new(0),
            pending_transactions: vec![0; QUEUE_SIZE],
            next_rslice_id: AtomicI64::new(0),
            last_rslice_id: AtomicI64::new(-1),
            pending_slices: vec![0; QUEUE_SIZE],
            fresh_val: vec![true; QUEUE_SIZE],
            r_ind : 0,
            w_ind : 0,
            sum: AtomicI64::new(0),
            epoch: AtomicI64::new(0),
        }
    }
}

impl<T:Default> Queue<T> {
    fn commit_tx(&mut self, tx_id: usize) {
        // commit the tx (do not finalize with 0 yet)
//        unsafe{std::ptr::write_volatile(&mut self.pending_transactions[tx_id],
 //                                       std::ptr::read_volatile(&self.pending_transactions[tx_id]) * -1)};
        self.pending_transactions[tx_id] *= -1;
        fence(Ordering::SeqCst);
        loop {
            let last_tx = self.last_commited_tx.load(Ordering::SeqCst);
            let mut cond = (last_tx + 1) % QUEUE_SIZE as i64 != tx_id as i64;
//            println!("tx_id  {}, expected tx_id {}, tail {}, shadow tail {}, head {}, shadow head {}", tx_id, last_tx + 1, self.tail.load(Ordering::SeqCst), self.shadow_tail.load(Ordering::SeqCst), self.head.load(Ordering::SeqCst), self.shadow_head.load(Ordering::SeqCst));
//            println!("tx_id  {}, expected tx_id {}", tx_id, last_tx + 1);
            // if we enter this condition, this tx is immediately after last commited tx
            if !cond {
                let mut max_tx_id = tx_id as i64;
                let mut sum = 0;
                while unsafe{std::ptr::read_volatile(&(self.pending_transactions[max_tx_id as usize]))} > 0 {
                    sum += unsafe{std::ptr::read_volatile(&self.pending_transactions[max_tx_id as usize])};
                    max_tx_id = (max_tx_id + 1) % QUEUE_SIZE as i64;
                }
                // the actual max_tx_id is the previous one
                max_tx_id = (max_tx_id - 1).rem_euclid(QUEUE_SIZE as i64);

                cond = self.last_commited_tx.compare_exchange(
                    last_tx, max_tx_id as i64, 
                    Ordering::SeqCst, Ordering::SeqCst
                ).is_ok();
                if cond {
                    // ensure that we don't commit transactions/ proceed tail 
                    // before the previous transactions also got commited
                    //assert!(self.pending_transactions[tx_id as usize] > 0);
                    loop {
                        if unsafe{std::ptr::read_volatile(&self.pending_transactions[(tx_id as i64 - 1).rem_euclid(QUEUE_SIZE as i64) as usize])} == 0 {
                            break;
                        }
                    }
                    // If the next expected transaction was commited after the above while loop
                    // was finished, we need to re-enter the loop and commit it 
                    if self.pending_transactions[((max_tx_id + 1) % QUEUE_SIZE as i64) as usize] > 0 {
                        cond = self.last_commited_tx.compare_exchange(
                            max_tx_id as i64, (tx_id as i64 - 1).rem_euclid(QUEUE_SIZE as i64), 
                            Ordering::SeqCst, Ordering::SeqCst
                        ).is_ok();
                        if cond {
                            continue;
                        }
                    }
                    // COMMIT transactions
                    let mut next_tx = tx_id as i64;
                    while next_tx != max_tx_id {
                        //unsafe{std::ptr::write_volatile(&mut self.pending_transactions[next_tx as usize], 0)};
                        self.pending_transactions[next_tx as usize] = 0;
                        next_tx = (next_tx + 1) % QUEUE_SIZE as i64;
                    }
                    self.pending_transactions[max_tx_id as usize] = 0;

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
                    break; // TODO: added for performance. check if causes problems
//                    println!("DEBUG last tx {}, last_commited_tx {}", last_tx, self.last_commited_tx.load(Ordering::SeqCst));
                }
            }
            else {
//                break;
//                println!("Expected tx : {}, curr tx {}", last_tx + 1, tx_id);
//                if last_tx == self.last_commited_tx.load(Ordering::SeqCst) {
                    break;
 //               }
//                println!("read tx : {}, now tx : {}", last_tx, self.last_commited_tx.load(Ordering::SeqCst));
            }
        }
    }

    pub fn reserve(&mut self, count: usize) -> Option<WritableSlice<T>> {
        // Check if there is enough space in the queue for the 
        // reservation count request
        let mut cur : usize;

        if count == 0 { 
            return None 
        }
        let mut tx_id : i64;
        loop {
            cur = self.shadow_tail.load(Ordering::SeqCst);
            if self.free_space() < count {
                return None; 
            }
            if self.shadow_tail.compare_exchange(
                cur, (cur + count) % QUEUE_SIZE, 
                Ordering::SeqCst, Ordering::SeqCst
            ).is_ok() {
                break;
            }
        }
        loop {
            if cur == self.expected_wslice_os.load(Ordering::SeqCst) as usize {
                loop {
                    tx_id = self.next_tx.load(Ordering::SeqCst);
                    if self.next_tx.compare_exchange(
                        tx_id, (tx_id + 1) % QUEUE_SIZE as i64, 
                        Ordering::SeqCst, Ordering::SeqCst
                    ).is_ok() {
                        break;
                    }        
                }        
                self.expected_wslice_os.store(
                    ((cur + count) % QUEUE_SIZE) as i64, Ordering::SeqCst
                );
                break;
            }
            else {
//                println!("Debug");
            }
        }
//        assert!(self.pending_transactions[tx_id as usize] == 0);
        unsafe{std::ptr::write_volatile(&mut self.pending_transactions[tx_id as usize], -(count as i64))};
        
//        println!("cur {}, tx_id {}, pending_trans {}", cur, tx_id, self.pending_transactions[tx_id as usize]);

//        println!("cur {}, tx_id {}, count {}, struct address {:p}", cur, tx_id, count, &self);
//        tx_id = (cur / WRITE_SLICE_S) as i64;
//        if tx_id < 2019 {
//            println!("cur {}, tx id {}", cur, tx_id);
//        }
//        assert!(self.pending_transactions[tx_id as usize] == 0);
        return Some(WritableSlice::new(self, cur, 0, tx_id as usize, count));
    }

    pub fn free_space(&self) -> usize {
        let head = self.head.load(Ordering::SeqCst);
        let s_tail = self.shadow_tail.load(Ordering::SeqCst) % QUEUE_SIZE;
        // after first time head will only become 
        // s_tail from read commits and not write commits 
        // (so QUEUE_SIZE is the correct value to return)
        let ret = if head <= s_tail { 
//            if head == s_tail {
//                println!("HI {}", head);
//            }
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
        let s_head = self.shadow_head.load(Ordering::SeqCst);
        let tail = self.tail.load(Ordering::SeqCst);

        let ret = if s_head <= tail {
            tail - s_head
        } else {
            QUEUE_SIZE - s_head + tail
        };
        //println!("Hi head {}, tail {} ", s_head, tail);

        return ret as usize;
    }

    pub fn dequeue_multiple(&mut self, count: i64) -> Option<Slice<T>> {
        let mut cur : usize;
        let mut len : usize;
        loop {
            cur = self.shadow_head.load(Ordering::SeqCst);
//            let cur_tail = self.tail.load(Ordering::SeqCst);
            let readable_amount = self.readable_amount();
//            let free_space = self.free_space();
//            let occupied_space = QUEUE_SIZE - free_space;
            len = cmp::min(readable_amount, count as usize);
            if len == 0 {
                return None;
            }
            if self.shadow_head.compare_exchange(
                cur, (cur + len) % QUEUE_SIZE, 
                Ordering::SeqCst, Ordering::SeqCst
            ).is_ok() {
//                println!("Occupied space {}, free space {}, head {}, s_head {}, s_tail {}, readable amount {}, tail {}", occupied_space, free_space, 
//                         self.head.load(Ordering::SeqCst), cur, self.shadow_tail.load(Ordering::SeqCst) % QUEUE_SIZE, readable_amount, self.tail.load(Ordering::SeqCst));
                break;
            }
        }
//        println!("{}", len);
        // loop probably not needed, check
        let mut s_id;
        loop {
            if cur == self.expected_rslice_os.load(Ordering::SeqCst) as usize {
                loop {
                    s_id = self.next_rslice_id.load(Ordering::SeqCst);
                    if self.next_rslice_id.compare_exchange(
                        s_id, (s_id + 1) % QUEUE_SIZE as i64, 
                        Ordering::SeqCst, Ordering::SeqCst
                    ).is_ok() {
                        break;
                    }
                }
//                    s_id = self.next_rslice_id.fetch_add(1, Ordering::SeqCst);
                self.expected_rslice_os.store(
                    ((cur + len) % QUEUE_SIZE) as i64, Ordering::SeqCst
                );
                break;
            }
            else {
//                    println!("Debug");
            }
        }
//        println!("offset {}, s_id {}", cur, s_id);
        //assert!(self.pending_slices[s_id as usize] == 0);
        self.pending_slices[s_id as usize] = -(len as i64);
        return Some(Slice{queue: self, offset: cur, slice_id: s_id, len: len});
    }
}
