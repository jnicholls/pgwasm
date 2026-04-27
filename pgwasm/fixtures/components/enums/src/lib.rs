wit_bindgen::generate!({
    path: "world.wit",
    world: "enums-fixture",
});

pub struct Component;

impl exports::corpus::en::sides::Guest for Component {
    fn const_left() -> exports::corpus::en::sides::Side {
        exports::corpus::en::sides::Side::Left
    }

    fn echo(s: exports::corpus::en::sides::Side) -> exports::corpus::en::sides::Side {
        s
    }
}

export!(Component);
