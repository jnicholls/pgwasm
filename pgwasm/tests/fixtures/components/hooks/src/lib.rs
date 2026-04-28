wit_bindgen::generate!({
    path: "world.wit",
    world: "hooks-fixture",
});

pub struct Component;

impl Guest for Component {
    fn on_reconfigure(_policy: String) -> Result<(), String> {
        Ok(())
    }
}

export!(Component);
