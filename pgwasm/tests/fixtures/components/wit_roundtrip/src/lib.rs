wit_bindgen::generate!({
    path: "world.wit",
    world: "wit-roundtrip",
});

pub struct Component;

impl Guest for Component {
    fn echo_bool(x: bool) -> bool {
        x
    }

    fn echo_s32(x: i32) -> i32 {
        x
    }

    fn echo_s64(x: i64) -> i64 {
        x
    }

    fn echo_string(x: String) -> String {
        x
    }
}

export!(Component);
