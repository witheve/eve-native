extern crate eve;

use eve::ops::{Program, CodeTransaction};
use eve::compiler::{parse_string};

//--------------------------------------------------------------------
// Helper macros
//--------------------------------------------------------------------

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
    // @FIXME: any occurrence of search/commit/etc. will be replaced here...
    let stringy = stringify!($info).replace("\n", " ")
        .replace("# ", "#")
        .replace("search", "\nsearch")
        .replace("commit", "\ncommit")
        .replace("bind", "\nbind")
        .replace("watch", "\nwatch")
        .replace("project", "\nproject")
        .replace("end", "\nend\n")
        .replace(" ! [", "[")
        .replace(" ! / ", "/")
        .replace(": =", ":=")
        .replace(" . ", ".");
    println!("{}", stringy);
    let blocks = parse_string(&mut program, &stringy, "test");
    let mut txn = CodeTransaction::new();
    txn.exec(&mut program, blocks, vec![]);

    program
}));

macro_rules! test (($name:ident, $body:tt) => (
    #[test]
    fn $name() {
        valid!($body);
    }

));

//--------------------------------------------------------------------
// string
//--------------------------------------------------------------------

test!(stdlib_string_replace_constants, {
    search
        new = string!/replace![text: "faoao" replace: "a" with: "b"]
    bind
        [#replaced new]
    end

    search
        [#replaced new: "fbobo"]
    bind
        [#success]
    end
});

test!(stdlib_string_replace_constants_with_empty, {
    search
        new = string!/replace![text: "faoao" replace: "a" with: ""]
    bind
        [#replaced new]
    end

    search
        [#replaced new: "foo"]
    bind
        [#success]
    end
});

test!(stdlib_string_replace_dynamic_text, {
    commit
        [#sample text: "foo"]
    end

    search
        [#sample text]
        new = string!/replace![text replace: "o" with: "e"]
    bind
        [#replaced new]
    end

    search
        [#replaced new: "fee"]
    bind
        [#success]
    end
});

test!(stdlib_string_replace_dynamic_replace_with, {
    commit
        [#replacements replace: "o" with: "e"]
        [#replacements replace: "f" with: "b"]
    end

    search
        [#replacements replace with]
        new = string!/replace![text: "foo" replace with]
    bind
        [#replaced new]
    end

    search
        [#replaced new: "fee"]
        [#replaced new: "boo"]
    bind
        [#success]
    end
});
