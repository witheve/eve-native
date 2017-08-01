#[macro_use]
extern crate eve;

use eve::ops::{Program, CodeTransaction};
use eve::compiler::{parse_string};

//--------------------------------------------------------------------
// math
//--------------------------------------------------------------------

test!(stdlib_math_floor, {
    search
        34 = math!/floor![value: 34.2]
    bind
        [#success]
    end
});

test!(stdlib_math_ceiling, {
    search
        35 = math!/ceiling![value: 34.2]
    bind
        [#success]
    end
});

test!(stdlib_math_round, {
    search
        34 = math!/round![value: 34.2]
    bind
        [#success]
    end
});

test!(stdlib_math_sin_degrees, {
    search
        math!/sin![degrees: 90]
    bind
        [#success]
    end
});

test!(stdlib_math_sin_radians, {
    search
        math!/sin![radians: 1.5]
    bind
        [#success]
    end
});

test!(stdlib_math_cos_degrees, {
    search
        math!/cos![degrees: 90]
    bind
        [#success]
    end
});

test!(stdlib_math_tan_degrees, {
    search
        math!/tan![degrees: 90]
    bind
        [#success]
    end
});

test!(stdlib_math_max, {
    search
        pac-man = 10
        donkey-kong = 13
        13 = math!/max![a: pac-man, b: donkey-kong]
    bind
        [#success]
    end
});

test!(stdlib_math_min, {
    search
        pac-man = 10
        donkey-kong = 13
        10 = math!/min![a: pac-man, b: donkey-kong]
    bind
        [#success]
    end
});

test!(stdlib_math_mod, {
    search
        1 = math!/mod![value: 5, by: 2]
    bind
        [#success]
    end
});

test!(stdlib_math_absolute, {
    search
        [#city name longitude]
        hours-from-gmt = math!/absolute![value: longitude] * 24 / 360 
    bind
        [#success]
    end

    commit
        [#city name: "Paris" longitude: 2.33]
        [#city name: "New York" longitude: -75.61]
        [#city name: "Los Angeles" longitude: -118.24]
    end
});

test!(stdlib_math_pow, {
    search
        8 = math!/pow![value: 2 exponent: 3]
    bind
        [#success]
    end
});

test!(stdlib_math_log, {
    search
        0 = math!/ln![value: 1]
    bind
        [#success]
    end
});

test!(stdlib_math_to_fixed, {
    search
        circumference = 6
        diameter = 1.910
        3.14 = math!/to!-fixed![value: (circumference / diameter), to: 2]
    bind
        [#success]
    end
});

test!(stdlib_math_to_range, {
    search
        y = math!/range![start: 1, stop: 10]
        10 = gather/count![for: y]
    bind
        [#success]
    end
});

test!(stdlib_random_number, {
    search
        x = random/number![seed: 3]
        y = random/number![seed: 3]
        x = y
    bind
        [#success]
    end
});

test!(stdlib_string_replace, {
    search
        string = "I love the flavour."
        "I love the flavor." = string!/replace![text: string, replace: "flavour", with: "flavor"]
    bind
        [#success]
    end
});

test!(stdlib_string_get, {
    search
        alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "Q" = string!/get![text: alphabet, at: 17]
    bind
        [#success]
    end
});

test!(stdlib_string_codepoint_length, {
    search
        7 = string!/codepoint!-length![text: "unicode"]
        2 = string!/codepoint!-length![text: "??"]
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
