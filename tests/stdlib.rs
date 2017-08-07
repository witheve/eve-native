#[macro_use]
extern crate eve;

use eve::ops::{Program, CodeTransaction};
use eve::compiler::{parse_string};

//--------------------------------------------------------------------
// math
//--------------------------------------------------------------------

test!(stdlib_math_range, {
    search
        value = math!/range![from:1 to:3]
    bind
        [#thing value]
    end

    search
        [#thing value:1]
        [#thing value:2]
        [#thing value:3]
        not([#thing value:4])
    bind
        [#success]
    end
});

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
