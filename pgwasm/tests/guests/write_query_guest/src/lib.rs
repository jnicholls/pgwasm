wit_bindgen::generate!({
    world: "guest",
    path: "wit",
    generate_all,
});

use crate::pgwasm::host::query;

struct WriteGuest;

impl Guest for WriteGuest {
    fn run() -> String {
        match query::read("DELETE FROM pg_class WHERE false", &[]) {
            Ok(_) => "OK".to_string(),
            Err(e) => e,
        }
    }
}

export!(WriteGuest);
