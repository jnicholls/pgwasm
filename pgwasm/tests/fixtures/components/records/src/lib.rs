wit_bindgen::generate!({
    path: "world.wit",
    world: "records-fixture",
});

pub struct Component;

impl exports::corpus::rec::math::Guest for Component {
    fn sum_fields(x: i32, y: i32) -> i32 {
        x.saturating_add(y)
    }
}

export!(Component);
