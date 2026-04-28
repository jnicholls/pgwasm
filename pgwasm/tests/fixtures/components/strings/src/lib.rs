wit_bindgen::generate!({
    path: "world.wit",
    world: "strings-fixture",
});

pub struct Component;

impl exports::corpus::str::bytes::Guest for Component {
    fn cat_bytes(a: Vec<u8>, b: Vec<u8>) -> Vec<u8> {
        [a, b].concat()
    }

    fn len_bytes(s: Vec<u8>) -> i32 {
        i32::try_from(s.len()).unwrap_or(i32::MAX)
    }
}

export!(Component);
