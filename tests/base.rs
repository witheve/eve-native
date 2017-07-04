extern crate eve;

use eve::ops::{Program};
use eve::parser::{parse_string};

// macro_rules! n (($p:ident, $i:expr) => ({ $p.state.interner.number_id($i as f32) }));
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
    let stringy = stringify!($info).replace("# ", "#").replace(" ! [", "[").replace(" ! / ", "/");
    let blocks = parse_string(&mut program, &stringy, "test");
    for block in blocks {
        program.raw_block(block);
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
    end

    search
        [#bar baz: [#zomg]]
    bind
        [#success]
    end

    commit
        [#foo woah: 1000]
    end
});

test!(base_bind_plus, {
    search
        [#foo woah]
    bind
        [#bar baz: woah + 10]
    end

    search
        [#bar baz: 1010]
    bind
        [#success]
    end

    commit
        [#foo woah: 1000]
    end
});

test!(base_no_scans, {
    search
        2 = 1 + 1
    bind
        [#success]
    end
});

test!(base_no_scans_fail, {
    search
        x = 1 + 1
        x != 3
    bind
        [#success]
    end
});

//--------------------------------------------------------------------
// Joins
//--------------------------------------------------------------------

test!(base_join_constant, {
    commit
        [#foo x: 3]
    end

    search
        x = 3
        [#foo x]
    bind
        [#success]
    end
});

test!(base_join_expression, {
    commit
        [#foo x: 3]
    end

    search
        x = 1 + 2
        [#foo x]
    bind
        [#success]
    end
});

test!(base_join_cross_different, {
    commit
        [#foo x: 3]
        [#bar y: "hi"]
    end

    search
        [#foo x: 3]
        [#bar y: "hi"]
    bind
        [#success]
    end
});

test!(base_join_cross_similar, {
    commit
        [#foo x: 3]
        [#foo x: 4]
    end

    search
        [#foo x: 3]
        [#foo x: 4]
    bind
        [#success]
    end
});

test!(base_join_many_attributes, {
    commit
        [#foo x: 3 y: "hi"]
    end

    search
        [#foo x: 3 y: "hi"]
    bind
        [#success]
    end
});

test!(base_join_many_values, {
    commit
        [#foo x: (3, 4)]
    end

    search
        [#foo x: (3, 4)]
    bind
        [#success]
    end
});


test!(base_join_binary, {
    commit
        [#foo x: 3]
        [#bar x: 3]
    end

    search
        [#foo x]
        [#bar x]
    bind
        [#success]
    end
});

test!(base_join_binary_multi, {
    commit
        [#foo x: (3, 4, 5)]
        [#bar y: (4, 5, 6)]
    end

    search
        [#foo x]
        [#bar y: x]
    bind
        [#success]
    end
});

test!(base_join_trinary, {
    commit
        [#foo x: 3]
        [#bar x: 3]
        [#baz x: 3]
    end

    search
        [#foo x]
        [#bar x]
        [#baz x]
    bind
        [#success]
    end
});

test!(base_join_transitive, {
    commit
        [#foo x: 3]
        [#bar x: 3 y: 5]
        [#baz y: 5 z: 8]
    end

    search
        [#foo x]
        [#bar x y]
        [#baz y z]
    bind
        [#success]
    end
});

test!(base_join_binary_unmatched, {
    commit
        [#foo x: 3]
        [#bar y: 4]
    end

    search
        [#foo x]
        [#bar y != x]
    bind
        [#success]
    end
});

//--------------------------------------------------------------------
// Interpolation
//--------------------------------------------------------------------

test!(base_interpolation_search_number, {
    search
        x = 1 + 1
        baz = "{{x}}"
    bind
        [#foo baz]
    end

    search
        [#foo baz: "2"]
    bind
        [#success]
    end
});

test!(base_interpolation_search_expression, {
    search
        baz = "{{1 + 2}}"
    bind
        [#foo baz]
    end

    search
        [#foo baz: "3"]
    bind
        [#success]
    end
});

test!(base_interpolation_search_multiple, {
    search
        x = 1
        y = 3.5
        baz = "({{x}}, {{y}})"
    bind
        [#foo baz]
    end

    search
        [#foo baz: "(1, 3.5)"]
    bind
        [#success]
    end
});

test!(base_interpolation_bind_string, {
    search
        x = "hi there!"
    bind
        [#foo baz: "{{x}}"]
    end

    search
        [#foo baz: "hi there!"]
    bind
        [#success]
    end
});


test!(base_interpolation_bind_number, {
    search
        x = 1 + 1
    bind
        [#foo baz: "{{x}}"]
    end

    search
        [#foo baz: "2"]
    bind
        [#success]
    end
});

test!(base_interpolation_bind_expression, {
    search
        x = 1 + 1
    bind
        [#foo baz: "{{x + 1}}"]
    end

    search
        [#foo baz: "3"]
    bind
        [#success]
    end
});

//--------------------------------------------------------------------
// MultiFunction
//--------------------------------------------------------------------

test!(base_multi_function, {
    search
        value = string!/split![text:"hey dude", by: " "]
    bind
        [#token value]
    end

    search
        [#token value: "hey"]
        [#token value: "dude"]
    bind
        [#success]
    end
});

//--------------------------------------------------------------------
// Not
//--------------------------------------------------------------------

test!(base_not, {
    search
        not([#foo])
    bind
        [#bar]
    end

    search
        [#bar]
    bind
        [#success]
    end
});

test!(base_not_reverse, {
    search
        not([#foo])
    bind
        [#bar]
    end

    commit
        [#foo]
    end

    search
        not([#bar])
    bind
        [#success]
    end
});

test!(base_not_no_join, {
    search
        [#zomg]
        not([#foo])
    bind
        [#success]
    end

    commit
        [#zomg]
    end
});

test!(base_not_no_join_retraction, {
    search
        [#zomg]
        not([#foo])
    bind
        [#bar]
    end

    commit
        [#zomg]
        [#foo]
    end

    search
        not([#bar])
    bind
        [#success]
    end
});

test!(base_not_join_f, {
    search
        z = [#zomg]
        not([#foo z])
    bind
        [#success]
    end

    commit
        [#zomg]
        [#foo z: 4]
    end
});

test!(base_not_join_retraction, {
    search
        z = [#zomg]
        not([#foo z])
    bind
        [#bar]
    end

    commit
        z = [#zomg]
        [#foo z]
    end

    search
        not([#bar])
    bind
        [#success]
    end
});

//--------------------------------------------------------------------
// Choose
//--------------------------------------------------------------------

test!(base_choose, {
    search
        [#foo x]
        z = if x = 3 then "medium"
            else if x = 10 then "large"
            else "too big"
    bind
        [#zomg x z]
    end

    commit
        [#foo x:3]
        [#foo x:10]
        [#foo x:100]
    end

    search
        [#zomg x:3 z:"medium"]
        [#zomg x:10 z:"large"]
        [#zomg x:100 z:"too big"]
    bind
        [#success]
    end
});

test!(base_choose_no_equality, {
    search
        [#foo x]
        z = if x > 3 then "large"
            else "small"
    bind
        [#zomg x z]
    end

    commit
        [#foo x:3]
        [#foo x:10]
        [#foo x:100]
    end

    search
        [#zomg x:3 z:"small"]
        [#zomg x:10 z:"large"]
        [#zomg x:100 z:"large"]
    bind
        [#success]
    end
});

test!(base_choose_multi_field, {
    search
        [#foo x]
        (a,b) = if x > 3 then ("large", "> 3")
                else ("small", "<= 3")
    bind
        [#zomg x a b]
    end

    commit
        [#foo x:3]
        [#foo x:10]
        [#foo x:100]
    end

    search
        [#zomg x:3 a:"small" b:"<= 3"]
        [#zomg x:10 a:"large" b:"> 3"]
        [#zomg x:100 a:"large" b:"> 3"]
    bind
        [#success]
    end
});

//--------------------------------------------------------------------
// Union
//--------------------------------------------------------------------

test!(base_union, {
    search
        [#foo x]
        z = if x > 3 then "large"
            if x = 10 then "woah"
    bind
        [#zomg x | z]
    end

    commit
        [#foo x:3]
        [#foo x:10]
        [#foo x:100]
    end

    search
        [#zomg x:10 z:("large", "woah")]
        [#zomg x:100 z:"large"]
        not([#zomg x:3])
    bind
        [#success]
    end
});
