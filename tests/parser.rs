extern crate eve;
use eve::ops::{Program};
use eve::compiler::*;
use eve::parser::*;
use eve::combinators::*;

//--------------------------------------------------------------------
// Helper macros
//--------------------------------------------------------------------

macro_rules! parse_blocks (($info:tt) => ({
    let mut program = Program::new();
    let stringy = stringify!($info).replace("# ", "#")
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
    let blocks = parse_string(&mut program, &stringy, "test");
    blocks
}));

macro_rules! test (($name:ident, $body:tt) => (
    #[test]
    fn $name() {
        parse_blocks!($body);
    }
));

//--------------------------------------------------------------------
// Parse errors
//--------------------------------------------------------------------

test!(parse_error_empty_search, {
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


#[test]
pub fn parser_combinator() {
    let mut state = ParseState::new("z = if x = 3 then \"medium\"
            else if x = 10 then \"large\"
            else \"too big\"
");

    let result = if_expression(&mut state);
    println!("{:?}", result);
}
