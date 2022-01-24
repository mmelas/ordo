use std::sync::{Arc, Mutex, Condvar};
use std::cell::UnsafeCell;
use std::{thread};
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Instant;
use crate::fifo;

// NUM_ITEMS must be multiple of 8
const NUM_ITEMS : usize = 10_000;
const PRODUCERS : i64 = 4;
const CONSUMERS : i64 = 4;
const WRITE_SLICE_S : usize = 200;
const READ_SLICE_S : usize = 100;

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

    let prod_sem = Arc::new(Semaphore::new(NUM_ITEMS as i64));
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
            let sem_p = prod_sem.clone();
            let sem_c = cons_sem.clone();
            prod_threads.push(thread::spawn(move || {
                loop {
                    let wslice = unsafe{ (*p.get()).reserve(WRITE_SLICE_S) };
                    match wslice {
                        Some(mut x) => {
                            sem_p.dec();
                            for _ in 0..x.len {
                                let curr = cnt_clone.fetch_add(1, Ordering::SeqCst);
                                unsafe {
                                    x.update(curr);
                                }
                            }
                            unsafe {
                                x.commit();
                            }
                            sem_c.inc();
                        },
                        None => {
                        //    println!("error");
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

 //   let vecs = UnsafeCell::new(vec![vec![]; PRODUCERS as usize]);
//    let ptr_vec = vecs.get();

    let rem_read = Arc::new(Mutex::new(NUM_ITEMS as i64));
    let t0 = Instant::now();
    for i in 0..CONSUMERS as usize {
        let p = SendPtr(ptr_wslice);
//        let safe_ptr_vec = SendPtr(ptr_vec);
//        let mut v = vec![0; READ_SLICE_S];
        let sem_p = prod_sem.clone();
        let sem_c = cons_sem.clone();
        let rem_c = rem_read.clone();
        cons_threads.push(thread::spawn(move || {
            loop {
                sem_c.dec();
                let mut slice = unsafe{ (*p.get()).dequeue_multiple(READ_SLICE_S as i64) };
                let offset = slice.offset;
                let mut v_ind = 0;
                let mut rem = rem_c.lock().unwrap();
                *rem -= slice.len as i64;
                if *rem <= 0 {
                    let consumers_time = t0.elapsed();
                    println!("Consumers time: {:.2?}", consumers_time);
                    println!("Producers time: {:.2?}", producers_time);
                    println!("Total time: {:.2?}", producers_time + consumers_time);
                    break;
                }
//                for i in 0..slice.len {
//                   v[v_ind] = slice.queue.buffer[i + offset] + 1;
//                    v_ind += 1;
//                }
//                unsafe { (*safe_ptr_vec.get())[i] = v };
                slice.commit();
                sem_p.inc();
            }
        }));
    }
    while true {
        //do nothing
    }

    //DO NOT push to v1, v2 etc simultaneously, put correct indices instead.
//    let consumers_time = t0.elapsed();
//    println!("Producers time: {:.2?}", producers_time);
//    println!("Consumers time: {:.2?}", consumers_time);
//    println!("Total time: {:.2?}", producers_time + consumers_time);

//    let mut included_nums = HashSet::new();
//    for vector in vecs.into_inner() {
 //       for elem in vector {
  //          included_nums.insert(elem);
//            println!("{}",elem);
 //       }
  //  }

 //   for i in 0..NUM_ITEMS as i64 {
 //       if !included_nums.contains(&(i + 1)) {
 //           println!("Error : Didn't find {} in the hashmap", i + 1);
 //       }
 //   }
    println!("Nice");
}
