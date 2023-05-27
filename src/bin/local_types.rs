// use std::collections::HashMap;


// struct Capture {
//     times: HashMap<&'static Marker, f64>
// }

// struct Cursor<'a> {
//     pub capture: &'a Capture,
//     pub marker: &'static Marker
// }

// struct Marker {

// }

// impl Marker {
//     const fn new(cursor: &Cursor) -> Self {
//         Self {}
//     }
//     const fn root() -> Self {
//         Self {}
//     }
// }

// macro_rules! mark {
//     ($parent:ident) => {
//         {
//             static X: Marker = Marker::new(&$parent);
//             Cursor{capture: &$parent.capture, marker: &X}
//         }
//     }
// }

// macro_rules! start {
//     ($parent:ident) => {
//         {
//             static X: Marker = Marker::root();
//             Cursor{capture: &$parent, marker: &X}
//         }
//     }
// }


// fn do_something(capture: &Capture) -> u64 {
//     let X = start!(capture);

//     let mut count = 0;

//     for ii in 0..(1 << 15) {
//         let Y = mark!(X);
//         for jj in 0..(1 << 15) {
//             let Z = mark!(Y);
//             count += ii;
//         }
//         for jj in 0..(1 << 15) {
//             let Z = mark!(Y);
//             count += ii;
//         }
//     }

//     return count;
// }




fn main() {

}

