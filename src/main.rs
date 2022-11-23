use tasks::manager::ServerManagerTask;
use utilities::{logger::*,logger};
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() {
    //Init logger
    logger!();
   
    let mut manager = ServerManagerTask::default();
    match manager.init() {

        Err(err) =>error!("Failed to initialize server manager with error: {}", err),
        Ok(_) => {
            if let Err(err) = manager.start() {
                error!("Failed to start server manager with error {}", err);
            }
        },
    }
}
