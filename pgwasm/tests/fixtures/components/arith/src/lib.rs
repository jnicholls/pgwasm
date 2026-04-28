wit_bindgen::generate!({
    path: "world.wit",
    world: "arith-fixture",
});

pub struct Component;

impl Guest for Component {
    fn add(a: i32, b: i32) -> i32 {
        a.saturating_add(b)
    }
}

export!(Component);
