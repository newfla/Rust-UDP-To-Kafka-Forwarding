use tasks::{manager::ServerManagerTask, AlwaysShouldGoOn};
use utilities::{logger::*,logger};

fn main() {
    
    //Init logger
    logger!();
   
    let mut manager = ServerManagerTask::default().should_go_on_strategy(Box::new(AlwaysShouldGoOn::default()));
    match manager.init() {

        Err(err) =>error!("Failed to initialize server manager with error: {}", err),
        Ok(_) => {
            if let Err(err) = manager.start() {
                error!("Failed to start server manager with error {}", err);
            }
        },
    }
}
