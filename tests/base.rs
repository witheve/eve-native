extern crate eve;

use eve::ops::{Program};

macro_rules! n (($p:ident, $i:expr) => ({ $p.interner.number_id($i as f32) }));
macro_rules! s (($p:ident, $i:expr) => ({ $p.interner.string_id(&$i) }));
// macro_rules! txn (($p:ident, [ $($t:ident ($e:ident, $a:expr, $v:expr),)* ]) => ({
//     let mut txn = Transaction::new();
//     $(txn.input(s!($p, "insert|".to_owned() + stringify!($e)), s!($p, $a), $t!($p, $v), 1);)*
//     txn.exec(&mut $p);
// }));
macro_rules! valid (($blocks:tt) => ({
    let mut program = blocks!($blocks);
    assert!(program.index.check(0, s!(program, "tag"), s!(program, "success")), "No success record");
}));

macro_rules! blocks (($info:tt) => ({
    let mut program = Program::new();
    let stringy = stringify!($info);
    let parts:Vec<&str> = stringy[1..stringy.len() - 1].split("~ ~ ~").collect();
    let mut ix = 0;
    for part in parts {
        let fixed = part.replace("# ", "#");
        program.block(&format!("block{}", ix), &format!("{}", fixed));
        ix += 1;
    }
    program
}));

macro_rules! test (($name:ident, $body:tt) => (
    #[test]
    fn $name() {
        valid!($body);
    }

));

test!(base_bind, {
    search
        [#foo woah]
    bind
        [#bar baz: [#zomg]]

    ~~~

    search
        [#bar baz: [#zomg]]
    bind
        [#success]

    ~~~

    commit
        [#foo woah: 1000]
});

test!(base_bind_plus, {
    search
        [#foo woah]
    bind
        [#bar baz: woah + 10]

    ~~~

    search
        [#bar baz: 1010]
    bind
        [#success]

    ~~~

    commit
        [#foo woah: 1000]
});
