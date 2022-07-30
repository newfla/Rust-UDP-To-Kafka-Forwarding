pub use log::{debug,error,info,log,trace,warn};
use std::sync::atomic::AtomicBool;

static mut INITIALIZED:AtomicBool = AtomicBool::new(false);

#[macro_export]
macro_rules! logger {
    ($a: expr) => {
        $crate::logger::init_logger(Some($a))
    };
    () => {
        $crate::logger::init_logger(None)
    };
}

/// This is an implementation detail and *should not* be called directly!
#[doc(hidden)]
pub fn init_logger(ovveride_level: Option<log::Level>){
    unsafe {
        if let false = *INITIALIZED.get_mut() {
            match ovveride_level {
                Some(lv) => simple_logger::init_with_level(lv),
                None => simple_logger::init_with_env(),
            }.expect("Error initializing logging utility");

            *INITIALIZED.get_mut() = true;
            info!("Logger succesfully initialized")
        }
    }
}

#[cfg(test)]
mod logger_tests {
    use log::{Level::{Debug,Trace},log_enabled};

    use crate::logger;

    #[test]
    fn test_logger() {
        logger!(Debug);
        assert!(log_enabled!(Debug));
        assert!(!log_enabled!(Trace));
    }
}