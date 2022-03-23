use crate::{process, fifo};
use crate::file_reader;
use crate::apply_regex;
use crate::output_results;

// NewType design in order to make
// raw pointer Send + Sync
//struct SendPtr<T> (*mut T);
//impl<T> SendPtr<T> {
//    pub fn get(self) -> *mut T {
//        return self.0;
//    }
//}
//unsafe impl<T> Send for SendPtr<T> {}
//unsafe impl<T> Sync for SendPtr<T> {}
//impl<T> Clone for SendPtr<T> {
//    fn clone(&self) -> Self { *self }
//}
//impl<T> Copy for SendPtr<T> {}

//pub fn read_file(iq : *mut fifo::Queue<String>, oq : *mut fifo::Queue<String>) {
//
//    let f_name = "test.txt";
//    let file = File::open(f_name).unwrap();
//    let map = UnsafeCell::new(unsafe{Mmap::map(&file).unwrap()});
//   
//    let file_size = unsafe{(*map.get()).len()};
//
//    let ptr = map.get();
//    
//    let to_split = unsafe{str::from_utf8(&(*ptr)[0..file_size])}.unwrap();
//    let splitted = to_split.split('\n').collect::<Vec<&str>>();
//    let total_lines = splitted.len() - 1;
////    println!("{}", total_lines);
//    let mut chunks : Vec<Vec<&str>> = Vec::new();
////    println!("{}", (total_lines / PRODUCERS) + 1);
//    let lines_per = if total_lines % PRODUCERS == 0 {total_lines / PRODUCERS} 
//                        else {(total_lines / PRODUCERS) + 1};
//    for chunk in splitted.chunks(lines_per) {
//        chunks.push(chunk.to_owned());
//    }
//
//    let mut threads = Vec::with_capacity(PRODUCERS);
//
//    for chunk in chunks {
//        let poq = SendPtr(oq);
//        // MUST RESERVE WRITE_SLICE_S slice size
//        let mut ws = unsafe{(*poq.get()).reserve(lines_per).unwrap()};
//        threads.push(thread::spawn(move || {
//            for line in chunk {
//                // to_owned will require copying under the hood, is it
//                // good if we work with str references instead?
//                unsafe{ws.update(line.to_owned())};
//            }
//            unsafe{ws.commit()};
//        }));
//    }
//
//    for th in threads {
//        let _ = th.join();
//    }
//}

pub fn run() {
    let pr = Box::leak(Box::new(process::ProcessRunner::new()));
    let q = Box::leak(Box::new(fifo::Queue{..Default::default()}));
    let q2 = Box::leak(Box::new(fifo::Queue{..Default::default()}));

    let mut p1 = file_reader::FileReader::new(q, q);
    p1.add_file("test0.txt".to_owned());
    p1.add_file("test1.txt".to_owned());
    p1.add_file("test2.txt".to_owned());
    p1.add_file("test3.txt".to_owned());

    let p2 = apply_regex::AppRegex::new(q, q2);

    let p3 = output_results::Output::new(q2, q2);


    pr.add_process(Box::new(p1));
    pr.add_process(Box::new(p2));
    pr.add_process(Box::new(p3));
    pr.start();

    loop {}
}
