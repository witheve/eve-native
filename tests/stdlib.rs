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
        .replace(" ! - ", "-")
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

test!(stdlib_string_index_of, {
    commit
        [needle: "a"]
        [needle: "bc"]
     end

    search
        [needle]
        ix = string!/index!-of![text: "abcaazbca" substring: needle]
    bind
        [#result ix]
    end

    search
        [#result ix: 1]
        [#result ix: 2]
        [#result ix: 4]
        [#result ix: 5]
        [#result ix: 7]
        [#result ix: 9]
    bind
        [#success]
    end
});

test!(stdlib_string_contains, {
    commit
        [#input text: "bleep"]
        [#input text: "sheep"]
        [#input text: "blap"]
    end

    search
        [#input text]
        string!/contains![text substring: "ee"]
    bind
        [#result text]
    end

    search
        [#result text: "bleep"]
        [#result text: "sheep"]
    bind
        [#success]
    end
});

test!(stdlib_string_uppercase, {
    commit
        [#input text: "BlEeP"]
        [#input text: "sheep"]
        [#input text: "CREEP"]
    end

    search
        [#input text]
        upper = string!/uppercase![text]
    bind
        [#result text: upper]
    end

    search
        [#result text: "BLEEP"]
        [#result text: "SHEEP"]
        [#result text: "CREEP"]
    bind
        [#success]
    end
});

test!(stdlib_string_lowercase, {
    commit
        [#input text: "BlEeP"]
        [#input text: "sheep"]
        [#input text: "CREEP"]
    end

    search
        [#input text]
        lower = string!/lowercase![text]
    bind
        [#result text: lower]
    end

    search
        [#result text: "bleep"]
        [#result text: "sheep"]
        [#result text: "creep"]
    bind
        [#success]
    end
});

test!(stdlib_string_length, {
    commit
        [#input text: "foo" expected: 3]
        [#input text: "a" expected: 1]
        [#input text: "" expected: 0]
        [#input text: "a̐éo " expected: 4]
    end

    search
        item = [#input text]
        length = string!/length![text]
    bind
        item.actual += length
    end

    search
        [#input text expected actual]
        expected != actual
    bind
        [#fail]
    end

    search
        not([#fail])
    bind
        [#success]
    end
});
