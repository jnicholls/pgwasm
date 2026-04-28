wit_bindgen::generate!({
    path: "world.wit",
    world: "variants-fixture",
});

pub struct Component;

impl exports::corpus::var::nums::Guest for Component {
    fn unwrap_or(m: exports::corpus::var::nums::MaybeNum, d: i32) -> i32 {
        match m {
            exports::corpus::var::nums::MaybeNum::None => d,
            exports::corpus::var::nums::MaybeNum::Some(v) => v,
        }
    }

    fn unwrap_or_plain(present: bool, v: i32, d: i32) -> i32 {
        if present { v } else { d }
    }
}

export!(Component);
