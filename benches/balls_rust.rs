// #![feature(test)]

// extern crate test;
// extern crate rand;

// use rand::{Rng, SeedableRng, XorShiftRng};
// use test::Bencher;

// type RecordId = usize;

// struct World {
//     records: Vec<Record>,
//     boid: Vec<RecordId>,
//     time: Vec<RecordId>,
//     canvas_root: Vec<RecordId>,
//     screen: Vec<RecordId>,
//     style: Vec<RecordId>,
//     arc: Vec<RecordId>,
//     canvas_path: Vec<RecordId>,
// }

// impl World {
//     pub fn new() -> World {
//         World {
//             records: vec![Record::Null],
//             boid: vec![],
//             time: vec![],
//             canvas_root: vec![],
//             screen: vec![],
//             style: vec![],
//             arc: vec![],
//             canvas_path: vec![],
//         }
//     }

//     pub fn store(&mut self, record:Record) -> usize {
//         let ix = self.records.len();
//         self.records.push(record);
//         ix
//     }

//     pub fn fetch(&mut self, id:RecordId) -> &mut Record {
//         &mut self.records[id]
//     }
// }

// enum Record {
//     Null,
//     Time { id: String, tags: Vec<String>, resolution: f64, },
//     CanvasRoot { id: String, tags: Vec<String>, width: f64, height: f64, style: RecordId },
//     Style { id: String, tags: Vec<String>, display: String, width: String, height: String, background_color: String },
//     Boid { id: String, tags: Vec<String>, order: usize, x: f64, y: f64, vx: f64, vy: f64, arc: RecordId, },
//     CanvasPath { id: String, tags: Vec<String>, sort: usize, children: Vec<RecordId> },
//     Arc { id: String, tags: Vec<String>, x:f64, y:f64, boid:RecordId, typ:String, sort: usize, radius: f64, start_angle: f64, end_angle: f64 },
// }

// fn make_time(world: &mut World) {
//     let ix = world.store(Record::Time { id: "time".to_string(), tags: vec!["time".to_string(), "system/timer".to_string()], resolution: 1000.0/60.0 });
//     world.time.push(ix);
// }

// fn make_canvas(world: &mut World) {
//     let style = world.store(Record::Style {
//         id: "style".to_string(),
//         tags: vec![],
//         display: "block".to_string(),
//         width: "500px".to_string(),
//         height: "500px".to_string(),
//         background_color: "red".to_string(),
//     });
//     let root = world.store(Record::CanvasRoot {
//         id: "root".to_string(),
//         tags: vec!["canvas/root".to_string(), "screen".to_string()],
//         width: 500.0,
//         height: 500.0,
//         style,
//     });
//     world.style.push(style);
//     world.canvas_root.push(root);
//     world.screen.push(root);
// }

// fn random(seed: u32) -> f64 {
//     let mut rng = XorShiftRng::from_seed([0x123, seed, !seed, seed]);
//    rng.next_f64()
// }

// fn make_balls(world: &mut World) {
//     for order in 1..200 {
//         let rand = random(order as u32);
//         let rand2 = random(order as u32);
//         let x = rand * 500.0;
//         let y = rand2 * 500.0;
//         let vx = rand * 3.0 + 1.0;
//         let vy = rand2 * 4.0 + 1.0;
//         let boid = world.store(Record::Boid { id: order.to_string(), tags: vec!["boid".to_string()], order: order as usize, arc: 0, x, y, vx, vy });
//         world.boid.push(boid);
//     }
// }

// fn add_screen_children(world: &mut World) {
//     for screen in world.screen.iter().cloned() {
//         for boid in world.boid.iter().cloned() {
//             let arc_id = world.records.len();
//             let (arc, path) = if let &mut Record::Boid { ref id, arc: ref mut boid_arc, order, .. } = &mut world.records[boid] {
//                 *boid_arc = arc_id;
//                 let arc = Record::Arc { id: format!("arc|{}", id), boid, tags: vec!["arc".to_string()], typ: "arc".to_string(), sort: 1, radius: 5.0, start_angle: 0.0, end_angle:2.0 * 3.14, x:0.0, y:0.0 };
//                 let path = Record::CanvasPath { id: format!("path|{}", order), tags: vec!["canvas/path".to_string()], sort: order, children: vec![arc_id] };
//                 (arc, path)
//             } else {
//                 panic!();
//             };
//             world.records.push(arc);
//             let path_id = world.records.len();
//             world.records.push(path);
//             world.arc.push(arc_id);
//             world.canvas_path.push(path_id);
//         }
//     }
// }

// fn on_tick(world: &mut World) {
//     for boid in world.boid.iter().cloned() {
//         let (arc, x, y) = if let &mut Record::Boid { ref mut x, ref mut y, ref mut vy, ref mut vx, arc, .. } = &mut world.records[boid] {
//             *x += *vx;
//             *y += *vy;
//             *vy += 0.07;
//             if *y < 10.0 && *vy < 0.0 {
//                 *vy *= -0.9;
//             }
//             if *x < 10.0 && *vx < 0.0 {
//                 *vx *= -0.9;
//             }
//             if *y > 490.0 && *vy > 0.0 {
//                 *vy *= -0.9;
//             }
//             if *x > 490.0 && *vx > 0.0 {
//                 *vx *= -0.9;
//             }
//             (arc, *x, *y)
//         } else {
//             unreachable!();
//         };
//         if let &mut Record::Arc { x:ref mut ax, y: ref mut ay, .. } = &mut world.records[arc] {
//             *ax = x;
//             *ay = y;
//         }
//     }

// }


// struct RawBoid {
//     x: f64,
//     y: f64,
//     vx: f64,
//     vy: f64,
//     arc: RawArc,
// }

// struct RawArc {
//     x: f64,
//     y: f64,
// }

// #[bench]
// fn rust_balls(b: &mut Bencher) {
//     let mut world = World::new();
//     make_balls(&mut world);
//     make_time(&mut world);
//     make_canvas(&mut world);
//     add_screen_children(&mut world);

//     b.iter(move || {
//         on_tick(&mut world);
//     });
// }

// #[bench]
// fn rust_balls_raw(b: &mut Bencher) {
//     let mut raw_boid = vec![];
//     for order in 1..200 {
//         let rand = random(order as u32);
//         let rand2 = random(order as u32);
//         let x = rand * 500.0;
//         let y = rand2 * 500.0;
//         let vx = rand * 3.0 + 1.0;
//         let vy = rand2 * 4.0 + 1.0;
//         raw_boid.push(RawBoid { x, y, vx, vy, arc: RawArc { x, y } });
//     }

//     b.iter(move || {
//         for boid in raw_boid.iter_mut() {
//             boid.x += boid.vx;
//             boid.y += boid.vy;
//             boid.vy += 0.07;
//             if boid.y < 10.0 && boid.vy < 0.0 {
//                 boid.vy *= -0.9;
//             }
//             if boid.x < 10.0 && boid.vx < 0.0 {
//                 boid.vx *= -0.9;
//             }
//             if boid.y > 490.0 && boid.vy > 0.0 {
//                 boid.vy *= -0.9;
//             }
//             if boid.x > 490.0 && boid.vx > 0.0 {
//                 boid.vx *= -0.9;
//             }
//             boid.arc.x = boid.x;
//             boid.arc.y = boid.y;
//         }
//     });
// }

