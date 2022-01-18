use std::sync::{Arc};
use std::cell::UnsafeCell;
use std::thread;
use std::sync::atomic::{AtomicI64, Ordering};
use std::collections::HashSet;
use std::time::Instant;
use crate::fifo;

// NUM_ITEMS must be multiple of 8
const NUM_ITEMS : usize = 100_000;
const THREADS : i64 = 8;
const THREAD_ITEMS : usize = NUM_ITEMS / THREADS as usize;

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

pub fn run_test() {
    let q = UnsafeCell::new(fifo::Queue{..Default::default()});

    /*
     * Each thread requires a different pointer ref
     */
    let ptr_wslice = q.get();
    let p = SendPtr(ptr_wslice);
    let p2 = SendPtr(ptr_wslice);
    let p3 = SendPtr(ptr_wslice);
    let p4 = SendPtr(ptr_wslice);
    let p5 = SendPtr(ptr_wslice);
    let p6 = SendPtr(ptr_wslice);
    let p7 = SendPtr(ptr_wslice);
    let p8 = SendPtr(ptr_wslice);

    /*
     * Each tread requires a different counter ref
     */
    let counter = Arc::new(AtomicI64::new(0));
    let cnt_clone = counter.clone();
    let cnt_clone2 = counter.clone();
    let cnt_clone3 = counter.clone();
    let cnt_clone4 = counter.clone();
    let cnt_clone5 = counter.clone();
    let cnt_clone6 = counter.clone();
    let cnt_clone7 = counter.clone();
    let cnt_clone8 = counter.clone();

    let t0 = Instant::now();
    {
        /*
         * Producers
         */
        let mut prod_threads = vec![];
        prod_threads.push(thread::spawn(move || {
            let wslice = unsafe{ (*p.get()).reserve(THREAD_ITEMS) };
            match wslice {
                Some(mut x) => {
                    for _ in 0..x.len {
                        let curr = cnt_clone.fetch_add(1, Ordering::SeqCst);
                        unsafe {
                            x.update(curr);
                        }
                    }
                    unsafe {
                        x.commit();
                    }
                },
                None => {
                    println!("error");
                }
            }
        }));

        prod_threads.push(thread::spawn(move || { 
            let wslice2 = unsafe{ (*p2.get()).reserve(THREAD_ITEMS) };
            match wslice2 {
                Some(mut x) => {
                    for _ in 0..x.len {
                        let curr = cnt_clone2.fetch_add(1, Ordering::SeqCst);
                        unsafe {
                            x.update(curr);
                        }
                    }
                    unsafe {
                        x.commit();
                    }
                },
                None => {
                    println!("error");
                }
            }
        }));

        prod_threads.push(thread::spawn(move || {
            let wslice3 = unsafe{ (*p3.get()).reserve(THREAD_ITEMS) };
            match wslice3 {
                Some(mut x) => {
                    for _ in 0..x.len {
                        let curr = cnt_clone3.fetch_add(1, Ordering::SeqCst);
                        unsafe {
                            x.update(curr);
                        }
                    }
                    unsafe {
                        x.commit();
                    }
                },
                None => {
                    println!("error");
                }
            }
        }));

        prod_threads.push(thread::spawn(move || {
            let wslice4 = unsafe{ (*p4.get()).reserve(THREAD_ITEMS) };
            match wslice4 {
                Some(mut x) => {
                    for _ in 0..x.len {
                        let curr = cnt_clone4.fetch_add(1, Ordering::SeqCst);
                        unsafe {
                            x.update(curr);
                        }
                    }
                    unsafe {
                        x.commit();
                    }
                },
                None => {
                    println!("error");
                }
            }
        }));

        prod_threads.push(thread::spawn(move || {
            let wslice5 = unsafe{ (*p5.get()).reserve(THREAD_ITEMS) };
            match wslice5 {
                Some(mut x) => {
                    for _ in 0..x.len {
                        let curr = cnt_clone5.fetch_add(1, Ordering::SeqCst);
                        unsafe {
                            x.update(curr);
                        }
                    }
                    unsafe {
                        x.commit();
                    }
                },
                None => {
                    println!("error");
                }
            }
        }));

        prod_threads.push(thread::spawn(move || {
            let wslice6 = unsafe{ (*p6.get()).reserve(THREAD_ITEMS) };
            match wslice6 {
                Some(mut x) => {
                    for _ in 0..x.len {
                        let curr = cnt_clone6.fetch_add(1, Ordering::SeqCst);
                        unsafe {
                            x.update(curr);
                        }
                    }
                    unsafe {
                        x.commit();
                    }
                },
                None => {
                    println!("error");
                }
            }
        }));

        prod_threads.push(thread::spawn(move || {
            let wslice7 = unsafe{ (*p7.get()).reserve(THREAD_ITEMS) };
            match wslice7 {
                Some(mut x) => {
                    for _ in 0..x.len {
                        let curr = cnt_clone7.fetch_add(1, Ordering::SeqCst);
                        unsafe {
                            x.update(curr);
                        }
                    }
                    unsafe {
                        x.commit();
                    }
                },
                None => {
                    println!("error");
                }
            }
        }));

        prod_threads.push(thread::spawn(move || {
            let wslice8 = unsafe{ (*p8.get()).reserve(THREAD_ITEMS) };
            match wslice8 {
                Some(mut x) => {
                    for _ in 0..x.len {
                        let curr = cnt_clone8.fetch_add(1, Ordering::SeqCst);
                        unsafe {
                            x.update(curr);
                        }
                    }
                    unsafe {
                        x.commit();
                    }
                },
                None => {
                    println!("error");
                }
            }
        }));


        for th in prod_threads {
            let _ = th.join();
        }
    }
    let producers_time = t0.elapsed();
    println!("Producers time: {:.2?}", producers_time);

    /*
     * Consumers
     */
    let mut cons_threads = vec![];
//    let contains = Arc::new(Mutex::new(HashMap::new()));
//    let contains_c = contains.clone();
//    let contains_c2 = contains.clone();
//    let contains_c3 = contains.clone();
//    let contains_c4 = contains.clone();
//    let contains_c5 = contains.clone();
//    let contains_c6 = contains.clone();
//    let contains_c7 = contains.clone();
//    let contains_c8 = contains.clone();

    let ptr_wslice = q.get();
    let p = SendPtr(ptr_wslice);
    let p2 = SendPtr(ptr_wslice);
    let p3 = SendPtr(ptr_wslice);
    let p4 = SendPtr(ptr_wslice);
    let p5 = SendPtr(ptr_wslice);
    let p6 = SendPtr(ptr_wslice);
    let p7 = SendPtr(ptr_wslice);
    let p8 = SendPtr(ptr_wslice);

    let mut v1 = vec![0; THREAD_ITEMS];
    let mut v2 = vec![0; THREAD_ITEMS];
    let mut v3 = vec![0; THREAD_ITEMS];
    let mut v4 = vec![0; THREAD_ITEMS];
    let mut v5 = vec![0; THREAD_ITEMS];
    let mut v6 = vec![0; THREAD_ITEMS];
    let mut v7 = vec![0; THREAD_ITEMS];
    let mut v8 = vec![0; THREAD_ITEMS];
    let vecs = UnsafeCell::new(vec![vec![]; THREADS as usize]);
    let ptr_vec = vecs.get();
    let safe_ptr_vec1 = SendPtr(ptr_vec);
    let safe_ptr_vec2 = SendPtr(ptr_vec);
    let safe_ptr_vec3 = SendPtr(ptr_vec);
    let safe_ptr_vec4 = SendPtr(ptr_vec);
    let safe_ptr_vec5 = SendPtr(ptr_vec);
    let safe_ptr_vec6 = SendPtr(ptr_vec);
    let safe_ptr_vec7 = SendPtr(ptr_vec);
    let safe_ptr_vec8 = SendPtr(ptr_vec);

    let t0 = Instant::now();
    {
        cons_threads.push(thread::spawn(move || {
            let mut slice = unsafe{ (*p.get()).dequeue_multiple(THREAD_ITEMS as i64) };
            let offset = slice.offset;
            let mut v_ind = 0;
            for i in 0..slice.len {
                v1[v_ind] = slice.queue.buffer[i + offset] + 1;
                v_ind += 1;
        //        println!("Iteration : {}, Item : {}", i, slice.queue.buffer[i + offset]);
            }
            unsafe { (*safe_ptr_vec1.get())[0] = v1 };
            slice.commit();
        }));

        cons_threads.push(thread::spawn(move || {
            let mut slice2 = unsafe{ (*p2.get()).dequeue_multiple(THREAD_ITEMS as i64) };
            let offset = slice2.offset;
            let mut v_ind = 0;
            for i in 0..slice2.len {
                v2[v_ind] = slice2.queue.buffer[i + offset] + 1;
                v_ind += 1;
        //        println!("Iteration : {}, Item : {}", i, slice2.queue.buffer[i +  offset]);
            }
            unsafe { (*safe_ptr_vec2.get())[1] = v2 };
            slice2.commit();
        }));

        cons_threads.push(thread::spawn(move || {
            let mut slice3 = unsafe{ (*p3.get()).dequeue_multiple(THREAD_ITEMS as i64) };
            let offset = slice3.offset;
            let mut v_ind = 0;
            for i in 0..slice3.len {
                v3[v_ind] = slice3.queue.buffer[i + offset] + 1;
                v_ind += 1;
         //       println!("Iteration : {}, Item : {}", i, slice3.queue.buffer[i + offset]);
            }
            unsafe { (*safe_ptr_vec3.get())[2] = v3 };
            slice3.commit();
        }));

        cons_threads.push(thread::spawn(move || {
            let mut slice4 = unsafe{ (*p4.get()).dequeue_multiple(THREAD_ITEMS as i64) };
            let offset = slice4.offset;
            let mut v_ind = 0;
            for i in 0..slice4.len {
                v4[v_ind] = slice4.queue.buffer[i + offset] + 1;
                v_ind += 1;
        //        println!("Iteration : {}, Item : {}", i, slice4.queue.buffer[i + offset]);
            }
            unsafe { (*safe_ptr_vec4.get())[3] = v4 };
            slice4.commit();
        }));

        cons_threads.push(thread::spawn(move || {
            let mut slice5 = unsafe{ (*p5.get()).dequeue_multiple(THREAD_ITEMS as i64) };
            let offset = slice5.offset;
            let mut v_ind = 0;
            for i in 0..slice5.len {
                v5[v_ind] = slice5.queue.buffer[i + offset] + 1;
                v_ind += 1;
                v5.push(slice5.queue.buffer[i + offset] + 1);
        //        println!("Iteration : {}, Item : {}", i, slice4.queue.buffer[i + offset]);
            }
            unsafe { (*safe_ptr_vec5.get())[4] = v5 };
            slice5.commit();
        }));

        cons_threads.push(thread::spawn(move || {
            let mut slice6 = unsafe{ (*p6.get()).dequeue_multiple(THREAD_ITEMS as i64) };
            let offset = slice6.offset;
            let mut v_ind = 0;
            for i in 0..slice6.len {
                v6[v_ind] = slice6.queue.buffer[i + offset] + 1;
                v_ind += 1;
        //        println!("Iteration : {}, Item : {}", i, slice4.queue.buffer[i + offset]);
            }
            unsafe { (*safe_ptr_vec6.get())[5] = v6 };
            slice6.commit();
        }));

        cons_threads.push(thread::spawn(move || {
            let mut slice7 = unsafe{ (*p7.get()).dequeue_multiple(THREAD_ITEMS as i64) };
            let offset = slice7.offset;
            let mut v_ind = 0;
            for i in 0..slice7.len {
                v7[v_ind] = slice7.queue.buffer[i + offset] + 1;
                v_ind += 1;
        //        println!("Iteration : {}, Item : {}", i, slice4.queue.buffer[i + offset]);
            }
            unsafe { (*safe_ptr_vec7.get())[6] = v7 };
            slice7.commit();
        }));

        cons_threads.push(thread::spawn(move || {
            let mut slice8 = unsafe{ (*p8.get()).dequeue_multiple(THREAD_ITEMS as i64) };
            let offset = slice8.offset;
            let mut v_ind = 0;
            for i in 0..slice8.len {
                v8[v_ind] = slice8.queue.buffer[i + offset] + 1;
                v_ind += 1;
        //        println!("Iteration : {}, Item : {}", i, slice4.queue.buffer[i + offset]);
            }
            unsafe { (*safe_ptr_vec8.get())[7] = v8 };
            slice8.commit();
        }));

        for th in cons_threads {
            let _ = th.join();
        }
    }
    //DO NOT push to v1, v2 etc simultaneously, put correct indices instead.
    let consumers_time = t0.elapsed();
    println!("Consumers time: {:.2?}", consumers_time);
    println!("Total time: {:.2?}", producers_time + consumers_time);

    let mut included_nums = HashSet::new();
    for vector in vecs.into_inner() {
        for elem in vector {
            included_nums.insert(elem);
//            println!("{}",elem);
        }
    }

    for i in 0..NUM_ITEMS as i64{
        if !included_nums.contains(&(i + 1)) {
            println!("Error : Didn't find {} in the hashmap", i + 1);
        }
    }
    println!("Nice");
}
