wit_bindgen::generate!({
    path: "world.wit",
    world: "resources-fixture",
});

use crate::exports::corpus::res::counter::{CounterBorrow, Guest, GuestCounter};

pub struct Component;

pub struct MyCounter;

impl GuestCounter for MyCounter {
    fn new() -> Self {
        MyCounter
    }

    fn bump(&self) -> i32 {
        1
    }
}

impl Guest for Component {
    type Counter = MyCounter;

    fn peek(_c: CounterBorrow<'_>) -> i32 {
        42
    }
}

export!(Component);
