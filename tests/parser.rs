extern crate eve;
use eve::ops::{Program};
use eve::parser::*;
use eve::parser2::*;

//--------------------------------------------------------------------
// Helper macros
//--------------------------------------------------------------------

macro_rules! parse_blocks (($info:tt) => ({
    let mut program = Program::new();
    let stringy = stringify!($info).replace("# ", "#").replace(" ! [", "[").replace(" ! / ", "/").replace(": =", ":=").replace(" . ", ".");
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
    this is pretty cool isn't it?

    search
    bind
    end
});


#[test]
pub fn parser_combinator() {
    let mut state = ParseState::new(" asdofk aspodkf
                                    search [#zomg] bind [#bar] end");
    let result = embedded_blocks(&mut state, "test.eve");
    println!("{:?}", result);
}
