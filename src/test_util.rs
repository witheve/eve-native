//--------------------------------------------------------------------
// Helper macros
//--------------------------------------------------------------------

// macro_rules! n (($p:ident, $i:expr) => ({ $p.state.interner.number_id($i as f32) }));
#[macro_export]
macro_rules! s (($p:ident, $i:expr) => ({ $p.state.interner.string_id(&$i) }));
// macro_rules! txn (($p:ident, [ $($t:ident ($e:ident, $a:expr, $v:expr),)* ]) => ({
//     let mut txn = Transaction::new();
//     $(txn.input(s!($p, "insert|".to_owned() + stringify!($e)), s!($p, $a), $t!($p, $v), 1);)*
//     txn.exec(&mut $p);
// }));
#[macro_export]
macro_rules! valid (($blocks:tt) => ({
    let mut program = blocks!($blocks);
    let a = s!(program, "tag");
    let v = s!(program, "success");
    let mut found = false;
    match program.state.index.get(0, a, v) {
        Some(mut iter) => {
            loop {
                match iter.next() {
                    Some(e) => {
                        if program.state.distinct_index.is_available(e,a,v) {
                            found = true;
                            break;
                        }
                    },
                    None => { break; }
                }
            }
        }
        None => {}
    }
    assert!(found, "No success record");
}));

#[macro_export]
macro_rules! blocks (($info:tt) => ({
    let mut program = Program::new("test");
    // @FIXME: any occurrence of search/commit/etc. will be replaced here...
    let stringy = stringify!($info).replace("\n", " ")
        .replace("# ", "#")
        .replace(" ! [", "[")
        .replace(" ! / ", "/")
        .replace(" ! - ", "-")
        .replace(" search", "\nsearch")
        .replace(" commit", "\ncommit")
        .replace(" bind", "\nbind")
        .replace(" watch", "\nwatch")
        .replace(" project", "\nproject")
        .replace(" end", "\nend\n")
        .replace(": =", ":=")
        .replace(" . ", ".");
    println!("{}", stringy);
    let blocks = parse_string(&mut program.state.interner, &stringy, "test");
    let mut txn = CodeTransaction::new();
    txn.exec(&mut program, blocks, vec![]);

    program
}));

#[macro_export]
macro_rules! test (($name:ident, $body:tt) => (
    #[test]
    fn $name() {
        valid!($body);
    }

));
