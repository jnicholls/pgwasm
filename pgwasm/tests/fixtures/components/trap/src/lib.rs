wit_bindgen::generate!({
    path: "world.wit",
    world: "trap-fixture",
});

pub struct Component;

impl Guest for Component {
    fn boom() {
        unreachable!();
    }
}

export!(Component);
