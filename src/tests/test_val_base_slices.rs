use std::sync::{Arc, Mutex, Condvar};
use std::cell::UnsafeCell;
use std::thread;
use std::sync::atomic::{AtomicI64, Ordering};
use std::collections::HashSet;
use std::time::Instant;
use crate::fifo;
use crate::params;

//const NUM_ITEMS : usize = params::new().NUM_ITEMS;
const NUM_ITEMS : usize = params::NUM_ITEMS;
const PRODUCERS : i64 = params::PRODUCERS;
const CONSUMERS : i64 = params::CONSUMERS;
const WRITE_SLICE_S : usize = params::WRITE_SLICE_S;
const READ_SLICE_S : usize = params::READ_SLICE_S;
const PRODUCERS_SLICES : usize = NUM_ITEMS / WRITE_SLICE_S;

// NewType design in order to make
// raw pointer Send + Sync
struct SendPtr<T> (*mut T);

impl<T> SendPtr<T> {
    pub fn get(self) -> *mut T {
        return self.0;
    }
}

unsafe impl<T> Send for SendPtr<T> {}
unsafe impl<T> Sync for SendPtr<T> {}

impl<T> Clone for SendPtr<T> {
    fn clone(&self) -> Self { *self }
}
impl<T> Copy for SendPtr<T> {}

pub struct Semaphore {
    mutex: Mutex<i64>,
    cvar: Condvar,
}

impl Semaphore {
    pub fn new(count: i64) -> Self {
        Semaphore {
            mutex: Mutex::new(count),
            cvar: Condvar::new(),
        }
    }

    pub fn dec(&self) {
        let mut lock = self.mutex.lock().unwrap();
        *lock -= 1;
        if *lock < 0 {
            let _ = self.cvar.wait(lock).unwrap();
        }
    }

    pub fn inc(&self) {
        let mut lock = self.mutex.lock().unwrap();
        *lock += 1;
        if *lock <= 0 {
            // maybe notify_all ? test performance of both
            self.cvar.notify_one();
        }
    }
}

unsafe impl Send for Semaphore {}
unsafe impl Sync for Semaphore {}

pub fn run_test() {
    let q = UnsafeCell::new(fifo::Queue{..Default::default()});

    /*
     * Each thread requires a different pointer ref
     */
    let ptr_wslice = q.get();

    /*
     * Each tread requires a different counter ref
     */
    let counter = Arc::new(AtomicI64::new(0));
    let slices_counter = Arc::new(AtomicI64::new(0));
    let prod_sem = Arc::new(Semaphore::new(WRITE_SLICE_S as i64));
    let cons_sem = Arc::new(Semaphore::new(0));


    let t0 = Instant::now();
    {
        /*
         * Producers
         */
        let mut prod_threads = vec![];
        for _ in 0..PRODUCERS {
            let p = SendPtr(ptr_wslice);
            let cnt_clone = counter.clone();
            let cnt_slices = slices_counter.clone();
            let sem_p = prod_sem.clone();
            let sem_c = cons_sem.clone();
            prod_threads.push(thread::spawn(move || {
                loop {
                    let wslice = unsafe{ (*p.get()).reserve(WRITE_SLICE_S) };
                    match wslice {
                        Some(mut x) => {
                            sem_p.dec();
                            let curr_cnt = cnt_slices.fetch_add(
                                1, Ordering::SeqCst
                            );
//                            println!("slice {}", curr_cnt);
                            let mut curr = 0;
                            for _ in 0..x.len {
                                curr = cnt_clone.fetch_add(
                                    1, Ordering::SeqCst
                                );
                                unsafe {
                                     x.update(curr);
                                 }
                            }
//                            if cnt_slices.load(Ordering::SeqCst) < 5000 {
//                                println!("{}", curr);
//                            }
                            unsafe {
                                x.commit();
                            }
                            sem_c.inc();
                            if curr_cnt >= PRODUCERS_SLICES as i64 - 1{
//                                println!("slice {}", cnt_slices.load(Ordering::SeqCst));
                                break;
                            }
                        },
                        None => {
//                            println!("error");
                        }
                    }
                }
            }));
        }
    }
    let producers_time = t0.elapsed();

    /*
     * Consumers
     */
    let mut cons_threads = vec![];

    let ptr_wslice = q.get();

    /*
     * Eventually the Set will contain 3*slice_size more values
     * because the producers do not stop appending values
     * and we have 4 threads reading simultaneously (so 3 threads more after 
     * the *rem = 0 one
     */
    let included_nums = Arc::new(Mutex::new(HashSet::new()));
    let rem_read = Arc::new(Mutex::new(NUM_ITEMS as i64));
    let t0 = Instant::now();
    for _ in 0..CONSUMERS as usize {
        let p = SendPtr(ptr_wslice);
        let rem_c = rem_read.clone();
        let included_nums_c = included_nums.clone();
        let sem_p = prod_sem.clone();
        let sem_c = cons_sem.clone();
        cons_threads.push(thread::spawn(move || {
            let mut cnt2 = 0;
            loop {
                sem_c.dec();
                let slice = unsafe{ 
                    (*p.get()).dequeue_multiple(READ_SLICE_S as i64) 
                };
                match slice {
                    Some(mut slice) => {
                        let offset = slice.offset;
                        for i in 0..slice.len {
                            let ind = (i + offset) % params::QUEUE_SIZE; // DO we need % here?
                            if included_nums_c.lock().unwrap().contains(
                                &(slice.queue.buffer[ind] + 1)
                            ) {
        //                        println!("Error, duplicate value {}", slice.queue.buffer[ind] + 1);
                                cnt2 += 1;
                            };
        //                    println!("Value {}", slice.queue.buffer[ind] + 1);
                            included_nums_c.lock().unwrap().insert(
                                slice.queue.buffer[ind] + 1
                            ); 
                        }
                        slice.commit();
        //                println!("len : {}, offset : {}, duplicate values {}", slice.len, offset, cnt2);
                        let mut rem = rem_c.lock().unwrap();
                        *rem -= slice.len as i64;
                        sem_p.inc();
                        if *rem <= 0 {
                            println!("Count of duplicates: {}", cnt2);
                            let consumers_time = t0.elapsed();
                            println!(
                                "Consumers time: {:.2?}", consumers_time
                            );
                            println!(
                                "Producers time: {:.2?}", producers_time
                            );
                            println!(
                                "Total time: {:.2?}", producers_time + consumers_time
                            );
                            let mut cnt_missing = 0;
                            for i in 0..NUM_ITEMS as i64 {
                                if !included_nums_c.lock().unwrap().contains(&(i + 1)) {
        //                            println!("{}, Error : Didn't find {} in the hashmap", *rem, i + 1);
                                    cnt_missing += 1;
                                }
                            }
                            println!(
                                "{}, Number of missing values : {}", *rem, cnt_missing
                            );
                            break;
                        }
                    },
                    None => {}
                }
            }
        }));
    }
    while true {
        //do nothing
    }

    println!("Nice");
}
