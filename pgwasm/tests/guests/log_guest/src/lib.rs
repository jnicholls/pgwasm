wit_bindgen::generate!({
    world: "guest",
    path: "wit",
    generate_all,
});

use crate::pgwasm::host::log::{self, Level};

struct LogGuest;

impl Guest for LogGuest {
    fn run() {
        log::log(Level::Notice, "hi");
    }
}

export!(LogGuest);
