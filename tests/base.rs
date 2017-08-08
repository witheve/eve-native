#[macro_use]
extern crate eve;

use eve::ops::{Program, CodeTransaction};
use eve::compiler::{parse_string};

//--------------------------------------------------------------------
// Basic binds
//--------------------------------------------------------------------

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

test!(base_join_records, {
    commit
        [#foo x: "a"]
        [#bar x: "a"]
    end

    search
        [#foo x]
        [#bar x]
    bind
        [#success]
    end
});

test!(base_join_nested_record, {
    commit
        [#bar x: "a"]
    end

    search
        bar = [#bar x]
    commit
        [#foo x: bar]
    end

    search
        bar = [#bar]
        [#foo x: bar]
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

test!(base_interpolation_functions, {
    search
        text = "Hello"
        replace = "o"
        with = "e"
        "Helle" = "{{ string/replace[text replace with] }}"
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

test!(base_multi_function_multi_field, {
    search
        (value, ix) = string!/split![text:"hey dude", by: " "]
    bind
        [#token value ix]
    end

    search
        [#token value: "hey" ix:1]
        [#token value: "dude" ix:2]
    bind
        [#success]
    end
});

test!(base_multi_function_multi_field_filtered, {
    search
        (value, 1) = string!/split![text:"hey dude", by: " "]
    bind
        [#token value]
    end

    search
        [#token value: "hey"]
        not([#token value: "dude"])
    bind
        [#success]
    end
});

test!(base_multi_function_multi_field_filtered_via_equality, {
    search
        (value, z) = string!/split![text:"hey dude", by: " "]
        z = 1
    bind
        [#token value]
    end

    search
        [#token value: "hey"]
        not([#token value: "dude"])
    bind
        [#success]
    end
});

test!(base_multi_function_multi_field_filtered_expression, {
    search
        (value, ix) = string!/split![text:"hey dude", by: " "]
        ix = 3 - 2
    bind
        [#token value]
    end

    search
        [#token value: "hey"]
        not([#token value: "dude"])
    bind
        [#success]
    end
});

//--------------------------------------------------------------------
// Eve auto index
//--------------------------------------------------------------------

test!(base_eve_auto_index, {
    commit
        [#div children:
            [#div1]
            [#div2]]
    end

    search
        [#div1 eve!-auto!-index: 1]
        [#div2 eve!-auto!-index: 2]
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

test!(base_choose_inequality, {
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

test!(base_choose_filtered, {
    search
        [#foo x]
        "large" = if x > 3 then "large"
                  else "small"
    bind
        [#zomg x]
    end

    commit
        [#foo x:3]
        [#foo x:10]
        [#foo x:100]
    end

    search
        [#zomg x:10]
        [#zomg x:100]
        not([#zomg x:3])
    bind
        [#success]
    end
});

test!(base_choose_filtered_multi_some, {
    search
        [#foo x]
        (10, z) = if x > 3 then (x, "large")
                  else ("unknown", "small")
    bind
        [#zomg x z]
    end

    commit
        [#foo x:3]
        [#foo x:10]
        [#foo x:100]
    end

    search
        [#zomg x:10 z:"large"]
        not([#zomg x:3])
        not([#zomg x:100])
    bind
        [#success]
    end
});

test!(base_choose_filtered_multi_some_via_equality, {
    search
        [#foo x]
        (a, z) = if x > 3 then (x, "large")
                  else ("unknown", "small")
        a = 10
    bind
        [#zomg x z]
    end

    commit
        [#foo x:3]
        [#foo x:10]
        [#foo x:100]
    end

    search
        [#zomg x:10 z:"large"]
        not([#zomg x:3])
        not([#zomg x:100])
    bind
        [#success]
    end
});

test!(base_choose_filtered_multi_all, {
    search
        [#foo x]
        (10, "large") = if x > 3 then (x, "large")
                        else ("unknown", "small")
    bind
        [#zomg x]
    end

    commit
        [#foo x:3]
        [#foo x:10]
        [#foo x:100]
    end

    search
        [#zomg x:10]
        not([#zomg x:3])
        not([#zomg x:100])
    bind
        [#success]
    end
});

test!(base_choose_filtered_multi_expression, {
    search
        [#foo x]
        (5 + 5, "large") = if x > 3 then (x, "large")
                           else ("unknown", "small")
    bind
        [#zomg x]
    end

    commit
        [#foo x:3]
        [#foo x:10]
        [#foo x:100]
    end

    search
        [#zomg x:10]
        not([#zomg x:3])
        not([#zomg x:100])
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

test!(base_choose_not_joinless, {
    search
        [#foo]
        a = if not([#app]) then "no app"
            else "with app"
    bind
        [#zomg a]
    end

    commit
        [#foo]
    end

    search
        [#zomg a:"no app"]
    bind
        [#success]
    end
});

test!(base_choose_not_joinless_failure, {
    search
        [#foo]
        a = if not([#app]) then "no app"
            else "with app"
    bind
        [#zomg a]
    end

    commit
        [#foo]
        [#app]
    end

    search
        [#zomg a:"with app"]
    bind
        [#success]
    end
});

test!(base_choose_lookup, {
    search
        f = [#foo]
        type = if lookup![entity: f] then "record"
               else "value"
    bind
        [#value type]
    end

    commit
        [#foo zomg: 4]
        [#app bar: 3]
    end

    search
        [#value type: "record"]
        not([#value type: "value"])
    bind
        [#success]
    end
});

test!(base_choose_lookup_rounds, {
    search
        [#foo value]
        type = if lookup![entity: value] then "record"
               else "value"
    bind
        [#value type]
    end

    commit
        [#foo value: [#zomg]]
        [#app bar: 3]
    end

    search
        f = [#foo]
    commit
        f.value := none
    end

    search
        f = [#foo]
        not(f.value)
    bind
        f.blah += "woot|a"
    end

    search
        f = [#foo blah]
    bind
        [#foo value: f]
        [#foo d:"yo" value: f]
    end

    search
        [#value type: "record"]
        not([#value type: "value"])
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

test!(base_union_else, {
    search
        [#foo x]
        z = if x > 10 then "large"
            else "small"
    commit
        [#zomg x | z]
    end

    commit
        [#foo x:3]
        [#foo x:10]
        [#foo x:100]
    end

    search
        [#zomg x:10 z: "small"]
        [#zomg x:100 z:"large"]
        [#zomg x:3 z: "small"]
    bind
        [#success]
    end
});

test!(base_union_multireturn, {
    search
        [#foo x]
        (z, y) = if x > 10 then ("large",3)
            else ("small", 4)
    commit
        [#zomg x | z y]
    end

    commit
        [#foo x:3]
        [#foo x:10]
        [#foo x:100]
    end

    search
        [#zomg x:10 z: "small" y: 4]
        [#zomg x:100 z:"large" y: 3]
        [#zomg x:3 z: "small" y: 4]
    bind
        [#success]
    end
});

test!(base_union_record, {
    search
        name = if [#foo first last] then "{{first}} {{last}}"
               if [#bar fullname] then fullname
    commit
        [#person name]
    end

    commit
        [#foo first: "Sam" last: "Smith"]
        [#bar fullname: "Leopold Hamburger"]
    end

    search
        sam = [#person name: "Sam Smith"]
        leo = [#person name: "Leopold Hamburger"]
    bind
        [#success]
    end
});

test!(base_union_record_multireturn, {
    search
        (name, person) = if p = [#foo first last] then ("{{first}} {{last}}", p)
                         if p = [#bar fullname] then (fullname, p)
    commit
        [#person name person]
    end

    commit
        [#foo first: "Sam" last: "Smith"]
        [#bar fullname: "Leopold Hamburger"]
    end

    search
        sam = [#person name: "Sam Smith" person: [#foo]]
        leo = [#person name: "Leopold Hamburger" person: [#bar]]
    bind
        [#success]
    end
});

//--------------------------------------------------------------------
// Update Operators
//--------------------------------------------------------------------

test!(base_update_add, {
    search
        foo = [#foo]
    bind
        foo.bar += "baz"
    end

    commit
        [#foo]
    end

    search
        [#foo bar: "baz"]
    bind
        [#success]
    end
});

test!(base_update_remove_last, {
    search
        foo = [#foo]
    commit
        foo.bar -= "baz"
    end

    commit
        [#foo bar: "baz"]
    end

    search
        foo = [#foo]
        not(foo.bar)
    bind
        [#success]
    end
});

test!(base_update_remove_one, {
    search
        foo = [#foo]
    commit
        foo.bar -= "fleeb"
    end

    commit
        [#foo bar: ("baz","fleeb")]
    end

    search
        [#foo bar]
        1 = gather!/count![for: bar]
    bind
        [#success]
    end
});

test!(base_update_set, {
    search
        foo = [#foo]
    commit
        foo.bar := "fleeb"
    end

    commit
        [#foo bar: "baz"]
    end

    search
        [#foo bar: "fleeb"]
    bind
        [#success]
    end
});

test!(base_update_set_none, {
    search
        foo = [#foo]
    commit
        foo.bar := none
    end

    commit
        [#foo bar: "baz"]
    end

    search
        foo = [#foo]
        not(foo.bar)
    bind
        [#success]
    end
});

test!(base_update_merge, {
    search
        foo = [#foo]
    commit
        foo <- [bar: "bar", baz: "baz"]
    end

    commit
        [#foo]
    end

    search
        [#foo bar baz]
    bind
        [#success]
    end
});

//--------------------------------------------------------------------
// Aggregates
//--------------------------------------------------------------------

test!(base_aggregate_sum, {
    search
        foo = [#foo value]
        total = gather!/sum![value, for:foo]
    bind
        [#total total]
    end

    commit
        [#foo value: 1]
        [#foo value: 2]
    end

    search
        [#total total: 3]
    bind
        [#success]
    end
});

test!(base_aggregate_sum_removal, {
    search
        foo = [#foo value]
        total = gather!/sum![value, for:foo]
    bind
        [#total total]
    end

    commit
        [#foo value: 1]
        [#foo value: 2]
    end

    search
        [#total total: 3]
        foo = [#foo value: 2]
    commit
        foo := none
    end

    search
        [#total total: 1]
        not([#total total: 3])
    bind
        [#success]
    end
});

test!(base_aggregate_count, {
    search
        foo = [#foo]
        total = gather!/count![for:foo]
    bind
        [#total total]
    end

    commit
        [#foo value: 1]
        [#foo value: 2]
    end

    search
        [#total total:2]
    bind
        [#success]
    end
});

test!(base_aggregate_count_remove, {
    search
        foo = [#foo]
        total = gather!/count![for:foo]
    bind
        [#total total]
    end

    commit
        [#foo value: 1]
        [#foo value: 2]
    end

    search
        [#total total: 2]
        foo = [#foo value: 2]
    commit
        foo := none
    end

    search
        [#total total:1]
        not([#total total:2])
    bind
        [#success]
    end
});

test!(base_aggregate_filtered, {
    search
        foo = [#foo]
        3 = gather!/count![for:foo]
    bind
        [#bar]
    end

    commit
        [#foo value: 1]
        [#foo value: 2]
    end

    search
        not([#bar])
    bind
        [#success]
    end
});

test!(base_aggregate_filtered_positive, {
    search
        foo = [#foo]
        3 = gather!/count![for:foo]
    bind
        [#bar]
    end

    commit
        [#foo value: 1]
        [#foo value: 2]
        [#foo value: 3]
    end

    search
        [#bar]
    bind
        [#success]
    end
});

test!(base_aggregate_filtered_join, {
    search
        [#expected total]
        foo = [#foo]
        total = gather!/count![for:foo]
    bind
        [#bar]
    end

    commit
        [#expected total: 3]
        [#foo value: 1]
        [#foo value: 2]
    end

    search
        not([#bar])
    bind
        [#success]
    end
});

test!(base_aggregate_filtered_join_positive, {
    search
        [#expected total]
        foo = [#foo]
        total = gather!/count![for:foo]
    bind
        [#bar]
    end

    commit
        [#expected total: 3]
        [#foo value: 1]
        [#foo value: 2]
        [#foo value: 3]
    end

    search
        [#bar]
    bind
        [#success]
    end
});

test!(base_aggregate_average, {
    search
        foo = [#foo value]
        total = gather!/average![value, for:foo]
    bind
        [#total total]
    end

    commit
        [#foo value: 1]
        [#foo value: 2]
    end

    search
        [#total total:1.5]
    bind
        [#success]
    end
});

test!(base_aggregate_average_remove, {
    search
        foo = [#foo value]
        total = gather!/average![value, for:foo]
    bind
        [#total total]
    end

    commit
        [#foo value: 1]
        [#foo value: 2]
    end

    search
        [#total total: 1.5]
        foo = [#foo value: 2]
    commit
        foo := none
    end

    search
        [#total total:1]
        not([#total total:1.5])
    bind
        [#success]
    end
});

test!(base_aggregate_transitive_dependencies, {
    search
        foo = [#foo value]
        value > 4
        total = gather!/count![for:foo]
    bind
        [#total total]
    end

    commit
        [#foo value: 1]
        [#foo value: 8]
    end

    search
        [#total total:1]
    bind
        [#success]
    end
});

test!(base_aggregate_transitive_choose, {
    search
        foo = [#foo]
        total = gather!/sum![value, for:foo]
        value = if foo.value then foo.value
                else 10
    bind
        [#total total]
    end

    commit
        [#foo]
        [#foo value: 8]
    end

    search
        [#total total:18]
    bind
        [#success]
    end
});

test!(base_aggregate_in_choose, {
    search
        foo = [#foo]
        total = if b = [#bar] then gather!/count![for: b]
                else 0
    bind
        [#total total]
    end

    commit
        [#foo]
        [#foo value: 8]
    end

    search
        [#total total:0]
    bind
        [#success]
    end
});

test!(base_aggregate_in_choose_valid, {
    search
        foo = [#foo]
        total = if b = [#bar] then gather!/count![for: b]
                else 0
    bind
        [#total total]
    end

    commit
        [#foo]
        [#foo value: 8]
        [#bar a: 1]
        [#bar a: 2]
        [#bar a: 3]
    end

    search
        [#total total:3]
    bind
        [#success]
    end
});

test!(base_aggregate_in_choose_remove, {
    search
        foo = [#foo]
        total = if b = [#bar] then gather!/count![for: b]
                else 0
    bind
        [#total total]
    end

    commit
        [#foo]
        [#foo value: 8]
        [#bar a: 1]
        [#bar a: 2]
        [#bar a: 3]
    end

    search
        [#total total:3]
        b = [#bar a: 1]
    commit
        b := none
    end

    search
        [#total total:2]
    bind
        [#success]
    end
});

test!(base_aggregate_in_choose_remove_and_add, {
    search
        foo = [#foo]
        total = if b = [#bar] then gather!/count![for: b]
                else 0
    bind
        [#total total]
    end

    commit
        [#foo]
        [#foo value: 8]
        [#bar a: 1]
        [#bar a: 2]
        [#bar a: 3]
    end

    search
        [#total total:3]
        b = [#bar a: 1]
    commit
        b := none
    end

    search
        [#total total:2]
    commit
        [#bar a: 4]
        [#bar a: 5]
    end

    search
        [#total total:4]
    bind
        [#success]
    end
});

test!(base_aggregate_in_choose_simple_rounds, {
    search
        foo = [#foo]
        total = if b = [#bar] then gather!/count![for: b]
                else 0
    bind
        [#total total]
    end

    commit
        [#foo]
    end

    search
        [#foo]
    bind
        [#bar value: 1]
    end

    search
        [#bar value]
        value < 5
    commit
        [#bar value: value + 1]
    end

    search
        [#total total:5]
    bind
        [#success]
    end
});

test!(base_aggregate_in_choose_rounds_retraction, {
    search
        foo = [#zomg]
        total = if b = [#bar] then gather!/count![for: b]
                else 0
    bind
        [#total | total]
    end

    commit
        [#foo]
        [#zomg]
    end

    search
        [#foo]
    bind
        [#bar value: 1]
    end

    search
        [#bar value]
        value < 5
    bind
        [#bar value: value + 1]
    end

    search
        [#bar value: 5]
        foo = [#foo]
    commit
        foo := none
    end

    search
        [#total total:0]
    bind
        [#success]
    end
});

test!(base_aggregate_top, {
    search
        foo = [#foo value]
        gather!/top![for:(value), limit:2]
    bind
        [#max foo]
    end

    commit
        [#foo value: 1]
        [#foo value: 2]
        [#foo value: 3]
        [#foo value: 4]
        [#foo value: 5]
    end

    search
        [#max foo: [value: 4]]
        [#max foo: [value: 5]]
    bind
        [#success]
    end
});

test!(base_aggregate_top_remove, {
    search
        foo = [#foo value]
        gather!/top![for:(value), limit:2]
    bind
        [#max foo]
    end

    commit
        [#foo value: 1]
        [#foo value: 2]
        [#foo value: 3]
        [#foo value: 4]
        [#foo value: 5]
    end

    search
        foo = [#foo value: 5]
    commit
        foo := none
    end

    search
        [#max foo: [value: 4]]
        [#max foo: [value: 3]]
    bind
        [#success]
    end
});

test!(base_aggregate_top_rounds, {
    search
        foo = [#foo value]
        gather!/top![for:(value), limit:2]
    bind
        [#max foo]
    end

    commit
        [#foo value: 1]
        [#foo value: 2]
        [#foo value: 3]
    end

    search
        [#foo value]
        value >= 3
        value < 6
    bind
        [#foo value: value + 1]
    end

    search
        [#max foo: [value: 5]]
        [#max foo: [value: 6]]
    bind
        [#success]
    end
});

test!(base_aggregate_top_rounds_removal, {
    search
        foo = [#foo value]
        gather!/top![for:(value), limit:2]
    bind
        [#max foo]
    end

    commit
        [#foo value: 1]
        [#foo value: 2]
        [#foo value: 3]
    end

    search
        [#foo value]
        value >= 3
        value < 6
    bind
        [#foo value: value + 1]
    end

    search
        foo = [#foo value: 3]
        [#foo value: 6]
    commit
        foo := none
    end

    search
        [#max foo: [value: 1]]
        [#max foo: [value: 2]]
    bind
        [#success]
    end
});

test!(base_aggregate_bottom, {
    search
        foo = [#foo value]
        gather!/bottom![for:(value), limit:2]
    bind
        [#max foo]
    end

    commit
        [#foo value: 1]
        [#foo value: 2]
        [#foo value: 3]
        [#foo value: 4]
        [#foo value: 5]
    end

    search
        [#max foo: [value: 1]]
        [#max foo: [value: 2]]
    bind
        [#success]
    end
});

test!(base_aggregate_bottom_remove, {
    search
        foo = [#foo value]
        gather!/bottom![for:(value), limit:2]
    bind
        [#max foo]
    end

    commit
        [#foo value: 1]
        [#foo value: 2]
        [#foo value: 3]
        [#foo value: 4]
        [#foo value: 5]
    end

    search
        foo = [#foo value: 1]
    commit
        foo := none
    end

    search
        [#max foo: [value: 2]]
        [#max foo: [value: 3]]
    bind
        [#success]
    end
});

test!(base_aggregate_bottom_rounds, {
    search
        foo = [#foo value]
        gather!/bottom![for:(value), limit:2]
    bind
        [#max foo]
    end

    commit
        [#foo value: 4]
        [#foo value: 5]
    end

    search
        [#foo value]
        value <= 4
        value > 1
    bind
        [#foo value: value - 1]
    end

    search
        [#max foo: [value: 1]]
        [#max foo: [value: 2]]
    bind
        [#success]
    end
});

test!(base_aggregate_bottom_rounds_removal, {
    search
        foo = [#foo value]
        gather!/bottom![for:(value), limit:2]
    bind
        [#max foo]
    end

    commit
        [#foo value: 4]
        [#foo value: 5]
        [#foo value: 6]
    end

    search
        [#foo value]
        value <= 4
        value > 1
    bind
        [#foo value: value - 1]
    end

    search
        foo = [#foo value:4]
        [#foo value:1]
    commit
        foo := none
    end

    search
        [#max foo: [value: 5]]
        [#max foo: [value: 6]]
    bind
        [#success]
    end
});
