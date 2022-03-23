mod test_base;
mod test_val_base;
mod test_multi;
mod test_val_multi;
mod test_val_base_slices;
mod test_base_slices;
mod params;
mod fifo;
mod process;
//mod test_process;
//mod test_procrunner;
mod peripherals;
mod log_parser;
mod file_reader;
mod apply_regex;
mod output_results;

fn main() {
//    test_base::run_test();
//    test_val_base::run_test();
//    test_multi::run_test();
//    test_val_multi::run_test();
//    test_val_base_slices::run_test();
//    test_base_slices::run_test();
//    test_process::run_test();
//    test_procrunner::run_test();
    log_parser::run();
}
