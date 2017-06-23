extern crate eve;

use eve::ops::{Program};

macro_rules! n (($p:ident, $i:expr) => ({ $p.state.interner.number_id($i as f32) }));
macro_rules! s (($p:ident, $i:expr) => ({ $p.state.interner.string_id(&$i) }));
// macro_rules! txn (($p:ident, [ $($t:ident ($e:ident, $a:expr, $v:expr),)* ]) => ({
//     let mut txn = Transaction::new();
//     $(txn.input(s!($p, "insert|".to_owned() + stringify!($e)), s!($p, $a), $t!($p, $v), 1);)*
//     txn.exec(&mut $p);
// }));
macro_rules! valid (($blocks:tt) => ({
    let mut program = blocks!($blocks);
    assert!(program.state.index.check(0, s!(program, "tag"), s!(program, "success")), "No success record");
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

test!(base_no_scans, {
    search
        2 = 1 + 1
    bind
        [#success]
});

test!(base_no_scans_fail, {
    search
        x = 1 + 1
        x != 3
    bind
        [#success]
});

//////////////////////////////////////////////////////////////////////
// Joins
//////////////////////////////////////////////////////////////////////

test!(base_join_constant, {
    commit
        [#foo x: 3]

    ~~~

    search
        x = 3
        [#foo x]
    bind
        [#success]
});

test!(base_join_expression, {
    commit
        [#foo x: 3]
    ~~~

    search
        x = 1 + 2
        [#foo x]
    bind
        [#success]
});

test!(base_join_cross_different, {
    commit
        [#foo x: 3]
        [#bar y: "hi"]

    ~~~

    search
        [#foo x: 3]
        [#bar y: "hi"]
    bind
        [#success]
});

test!(base_join_cross_similar, {
    commit
        [#foo x: 3]
        [#foo x: 4]

    ~~~

    search
        [#foo x: 3]
        [#foo x: 4]
    bind
        [#success]
});

test!(base_join_many_attributes, {
    commit
        [#foo x: 3 y: "hi"]

    ~~~

    search
        [#foo x: 3 y: "hi"]
    bind
        [#success]
});

test!(base_join_many_values, {
    commit
        [#foo x: (3, 4)]

    ~~~

    search
        [#foo x: (3, 4)]
    bind
        [#success]
});


test!(base_join_binary, {
    commit
        [#foo x: 3]
        [#bar x: 3]

    ~~~

    search
        [#foo x]
        [#bar x]
    bind
        [#success]
});

test!(base_join_binary_multi, {
    commit
        [#foo x: (3, 4, 5)]
        [#bar y: (4, 5, 6)]

    ~~~

    search
        [#foo x]
        [#bar y: x]
    bind
        [#success]
});

test!(base_join_trinary, {
    commit
        [#foo x: 3]
        [#bar x: 3]
        [#baz x: 3]

    ~~~

    search
        [#foo x]
        [#bar x]
        [#baz x]
    bind
        [#success]
});

test!(base_join_transitive, {
    commit
        [#foo x: 3]
        [#bar x: 3 y: 5]
        [#baz y: 5 z: 8]

    ~~~

    search
        [#foo x]
        [#bar x y]
        [#baz y z]
    bind
        [#success]
});


test!(base_join_binary_unmatched, {
    commit
        [#foo x: 3]
        [#bar y: 4]

    ~~~

    search
        [#foo x]
        [#bar y != x]
    bind
        [#success]
});

//////////////////////////////////////////////////////////////////////
// Interpolation
//////////////////////////////////////////////////////////////////////

test!(base_interpolation_search_number, {
    search
        x = 1 + 1
        baz = "{{x}}"
    bind
        [#foo baz]

    ~~~

    search
        [#foo baz: "2"]
    bind
        [#success]
});

test!(base_interpolation_search_expression, {
    search
        baz = "{{1 + 2}}"
    bind
        [#foo baz]

    ~~~

    search
        [#foo baz: "3"]
    bind
        [#success]
});

test!(base_interpolation_search_multiple, {
    search
        x = 1
        y = 3.5
        baz = "({{x}}, {{y}})"
    bind
        [#foo baz]

    ~~~

    search
        [#foo baz: "(1, 3.5)"]
    bind
        [#success]
});

test!(base_interpolation_bind_string, {
    search
        x = "hi there!"
    bind
        [#foo baz: "{{x}}"]

    ~~~

    search
        [#foo baz: "hi there!"]
    bind
        [#success]
});


test!(base_interpolation_bind_number, {
    search
        x = 1 + 1
    bind
        [#foo baz: "{{x}}"]

    ~~~

    search
        [#foo baz: "2"]
    bind
        [#success]
});

test!(base_interpolation_bind_expression, {
    search
        x = 1 + 1
    bind
        [#foo baz: "{{x + 1}}"]

    ~~~

    search
        [#foo baz: "3"]
    bind
        [#success]
});
