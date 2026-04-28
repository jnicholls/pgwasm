wit_bindgen::generate!({
    path: "world.wit",
    world: "policy-probe-fixture",
});

pub struct Component;

impl Guest for Component {
    fn ping() -> i32 {
        1
    }
}

export!(Component);
