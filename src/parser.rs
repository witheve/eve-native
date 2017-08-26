use compiler::{Node, OutputType};
use std::str::FromStr;
use combinators::*;
use error::{ParseError};

//--------------------------------------------------------------------
// Constants
//--------------------------------------------------------------------

const BREAK_CHARS:&'static str = "#\\.,()[]{}:=\"|; \r\n\t";
const BREAK_CHARS_AND_NUMBERS:&'static str = "012345679#\\.,()[]{}:=\"|; \r\n\t";

//--------------------------------------------------------------------
// Identifiers and variables
//--------------------------------------------------------------------

whitespace_parser!(identifier(state) -> Node<'a> {
    state.eat_space();
    let start = state.pos;
    one_except!(state, BREAK_CHARS_AND_NUMBERS);
    match state.consume_except(BREAK_CHARS) {
        _ => {}
    }
    pos_result!(state, Node::Identifier(state.capture(start)))
});

whitespace_parser!(variable(state) -> Node<'a> {
    state.eat_space();
    let start = state.pos;
    one_except!(state, BREAK_CHARS_AND_NUMBERS);
    match state.consume_except(BREAK_CHARS) {
        _ => {}
    }
    pos_result!(state, Node::Variable(state.capture(start)))
});

//--------------------------------------------------------------------
// Numbers
//--------------------------------------------------------------------

whitespace_parser!(float(state) -> Node<'a> {
    state.eat_space();
    let start = state.pos;
    // -? [0-9]+ \. [0-9]+
    any!(state, "-"); take_while_1!(state, is_digit); tag!(state, "."); take_while_1!(state, is_digit);
    let number = f32::from_str(state.capture(start)).unwrap();
    pos_result!(state, Node::Float(number))
});

whitespace_parser!(integer(state) -> Node<'a> {
    state.eat_space();
    let start = state.pos;
    // -? [0-9]+
    any!(state, "-"); take_while_1!(state, is_digit);
    let digits = state.capture(start);
    if let Ok(number) = i32::from_str(digits) {
        pos_result!(state, Node::Integer(number))
    } else {
        state.error(ParseError::NumberOverflow())
    }
});

parser!(number(state) -> Node<'a> {
    let num = alt!(state, [float integer]);
    result!(state, num)
});

//--------------------------------------------------------------------
// Strings
//--------------------------------------------------------------------

whitespace_parser!(escaped_quote(state) -> Node<'a> {
    tag!(state, "\\");
    let escaped = alt_tag!(state, ["\"" "n" "t"]);
    let ch = match escaped {
        "n" => "\n",
        "t" => "\t",
        _ => escaped
    };
    result!(state, Node::RawString(ch))
});

whitespace_parser!(string_embed(state) -> Node<'a> {
    tag!(state, "{{");
    let embed = call!(state, expression);
    state.eat_space();
    tag!(state, "}}");
    result!(state, embed)
});

whitespace_parser!(string_bracket(state) -> Node<'a> {
    tag!(state, "{");
    result!(state, Node::RawString("{"))
});

whitespace_parser!(string_chars(state) -> Node<'a> {
    let chars = any_except!(state, "\"{");
    result!(state, Node::RawString(chars))
});

whitespace_parser!(string_parts(state) -> Node<'a> {
    let part = alt!(state, [ escaped_quote string_embed string_bracket string_chars ]);
    result!(state, part)
});

parser!(string(state) -> Node<'a> {
    tag!(state, "\"");
    let mut parts = many!(state, string_parts);
    tag!(state, "\"");
    let result = match (parts.len(), parts.get(0)) {
        (1, Some(&Node::RawString(_))) => parts.pop().unwrap(),
        (0, None) => Node::RawString(""),
        _ => Node::EmbeddedString(None, parts)
    };
    pos_result!(state, result)
});

//--------------------------------------------------------------------
// values and expressions
//--------------------------------------------------------------------

parser!(none_value(state) -> Node<'a> {
    tag!(state, "none");
    pos_result!(state, Node::NoneValue)
});

parser!(value(state) -> Node<'a> {
    let part = alt!(state, [ number string record_function record_reference wrapped_expression ]);
    result!(state, part)
});

parser!(wrapped_expression(state) -> Node<'a> {
    tag!(state, "(");
    let value = call!(state, expression);
    tag!(state, ")");
    result!(state, value)
});

parser!(expression(state) -> Node<'a> {
    let part = alt!(state, [ infix_addition infix_multiplication value ]);
    result!(state, part)
});

parser!(expression_set(state) -> Node<'a> {
    tag!(state, "(");
    let exprs = many_1!(state, expression => EmptyUpdate);
    tag!(state, ")");
    pos_result!(state, Node::ExprSet(exprs))
});

//--------------------------------------------------------------------
// Infix
//--------------------------------------------------------------------

whitespace_parser!(infix_addition(state) -> Node<'a> {
    let left = alt!(state, [ infix_multiplication value ]);
    tag!(state, " ");
    let op = alt_tag!(state, [ "+" "-" ]);
    tag!(state, " ");
    let right = call!(state, expression);
    pos_result!(state, Node::Infix { result:None, left:Box::new(left), right:Box::new(right), op })
});

whitespace_parser!(infix_multiplication(state) -> Node<'a> {
    let left = call!(state, value);
    tag!(state, " ");
    let op = alt_tag!(state, [ "*" "/" ]);
    tag!(state, " ");
    let right = alt!(state, [ infix_multiplication value ]);
    pos_result!(state, Node::Infix { result:None, left:Box::new(left), right:Box::new(right), op })
});

parser!(equality(state) -> Node<'a> {
    let left = call!(state, expression);
    tag!(state, "=");
    let right = alt!(state, [ expression record ]);
    pos_result!(state, Node::Equality { left:Box::new(left), right:Box::new(right) })
});

parser!(inequality(state) -> Node<'a> {
    let left = call!(state, expression);
    let op = alt_tag!(state, [ ">=" "<=" "!=" "<" ">" ]);
    let right = call!(state, expression);
    pos_result!(state, Node::Inequality { left:Box::new(left), right:Box::new(right), op })
});

//--------------------------------------------------------------------
// Tags, Attributes
//--------------------------------------------------------------------

parser!(hashtag(state) -> Node<'a> {
    tag!(state, "#");
    let name = match call!(state, identifier).unwrap_pos() {
        Node::Identifier(v) => v,
        _ => unreachable!(),
    };
    pos_result!(state, Node::Tag(name))
});

parser!(attribute_variable(state) -> Node<'a> {
    let attr = match call!(state, identifier).unwrap_pos() {
        Node::Identifier(v) => v,
        _ => unreachable!(),
    };
    pos_result!(state, Node::Attribute(attr))
});

parser!(attribute_equality(state) -> Node<'a> {
    let attr = match call!(state, identifier).unwrap_pos() {
        Node::Identifier(v) => v,
        _ => unreachable!(),
    };
    alt_tag!(state, [ ":" "=" ]);
    let value = alt!(state, [ record_set wrapped_record_set expression expression_set ]);
    pos_result!(state, Node::AttributeEquality(attr, Box::new(value)))
});

parser!(attribute_inequality(state) -> Node<'a> {
    let attribute = match call!(state, identifier).unwrap_pos() {
        Node::Identifier(v) => v,
        _ => unreachable!(),
    };
    let op = alt_tag!(state, [ ">=" "<=" "!=" "<" ">" ]);
    let right = call!(state, expression);
    pos_result!(state, Node::AttributeInequality { attribute, right:Box::new(right), op })
});

parser!(attribute(state) -> Node<'a> {
    let part = alt!(state, [ hashtag attribute_equality attribute_inequality attribute_variable ]);
    result!(state, part)
});

parser!(pipe(state) -> Node<'a> {
    tag!(state, "|");
    pos_result!(state, Node::Pipe)
});

parser!(output_attribute(state) -> Node<'a> {
    let item = alt!(state, [ hashtag attribute_equality pipe attribute_variable ]);
    result!(state, item)
});

//--------------------------------------------------------------------
// Records
//--------------------------------------------------------------------

parser!(record(state) -> Node<'a> {
    tag!(state, "[");
    if state.output_type == OutputType::Lookup {
        let attributes = many!(state, attribute);
        tag!(state, "]");
        pos_result!(state, Node::Record(None, attributes))
    } else {
        let attributes = many!(state, output_attribute);
        tag!(state, "]");
        pos_result!(state, Node::OutputRecord(None, attributes, state.output_type))
    }
});

parser!(record_set(state) -> Node<'a> {
    let records = many_1!(state, record);
    pos_result!(state, Node::RecordSet(records))
});

parser!(wrapped_record_set(state) -> Node<'a> {
    tag!(state, "(");
    let set = call!(state, record_set);
    tag!(state, ")");
    result!(state, set)
});

//--------------------------------------------------------------------
// Functions and lookup
//--------------------------------------------------------------------

parser!(function_attribute(state) -> Node<'a> {
    let part = alt!(state, [ attribute_equality attribute_variable ]);
    result!(state, part)
});

parser!(lookup(state) -> Node<'a> {
    tag!(state, "lookup[");
    let attributes = many!(state, function_attribute);
    tag!(state, "]");
    pos_result!(state, Node::Lookup(attributes, state.output_type))
});

parser!(lookup_commit(state) -> Node<'a> {
    tag!(state, "lookup-commit[");
    let attributes = many!(state, function_attribute);
    tag!(state, "]");
    pos_result!(state, Node::LookupCommit(attributes))
});

parser!(lookup_remote(state) -> Node<'a> {
    tag!(state, "lookup-remote[");
    let attributes = many!(state, function_attribute);
    tag!(state, "]");
    pos_result!(state, Node::LookupRemote(attributes, state.output_type))
});

whitespace_parser!(record_function(state) -> Node<'a> {
    state.eat_space();
    let op = match call!(state, identifier).unwrap_pos() {
        Node::Identifier(v) => v,
        _ => unreachable!(),
    };
    tag!(state, "[");
    let params = many!(state, function_attribute);
    state.eat_space();
    tag!(state, "]");
    pos_result!(state, Node::RecordFunction { op, params, outputs:vec![] })
});

parser!(multi_equality_left(state) -> Node<'a> {
    let part = call!(state, expression_set);
    result!(state, part)
});

parser!(multi_function_equality(state) -> Node<'a> {
    let neue_outputs = match call!(state, multi_equality_left).unwrap_pos() {
        Node::ExprSet(items) => items,
        _ => unreachable!()
    };
    tag!(state, "=");
    let mut func = call!(state, record_function);
    match func {
        Node::Pos(_, box Node::RecordFunction { ref mut outputs, .. }) => {
           *outputs = neue_outputs;
        }
        _ => unreachable!()
    };
    result!(state, func)
});

//--------------------------------------------------------------------
// Attribute access (foo.bar)
//--------------------------------------------------------------------

parser!(dot_pair(state) -> Node<'a> {
    tag!(state, ".");
    let ident = call!(state, identifier);
    result!(state, ident)
});

parser!(attribute_access(state) -> Node<'a> {
    let start = match call!(state, identifier).unwrap_pos() {
        Node::Identifier(v) => v,
        _ => unreachable!(),
    };
    let mut items = vec![start];
    let mut pairs = many_1!(state, dot_pair);
    items.extend(pairs.drain(..).map(|x| {
        if let Node::Identifier(v) = x.unwrap_pos() { v } else { unreachable!() }
    }));
    pos_result!(state, Node::AttributeAccess(items))
});

parser!(record_reference(state) -> Node<'a> {
    let part = alt!(state, [ attribute_access variable ]);
    result!(state, part)
});

parser!(mutating_attribute_access(state) -> Node<'a> {
    let start = match call!(state, identifier).unwrap_pos() {
        Node::Identifier(v) => v,
        _ => unreachable!(),
    };
    let mut items = vec![start];
    let mut pairs = many_1!(state, dot_pair);
    items.extend(pairs.drain(..).map(|x| {
        if let Node::Identifier(v) = x.unwrap_pos() { v } else { unreachable!() }
    }));
    pos_result!(state, Node::MutatingAttributeAccess(items))
});

parser!(mutating_record_reference(state) -> Node<'a> {
    let part = alt!(state, [ mutating_attribute_access variable ]);
    result!(state, part)
});

//--------------------------------------------------------------------
// Outputs
//--------------------------------------------------------------------

parser!(update_add(state) -> Node<'a> {
    let left = call!(state, mutating_record_reference);
    tag!(state, "+=");
    let value = alt!(state, [ record record_set wrapped_record_set expression expression_set hashtag ]);
    pos_result!(state, Node::RecordUpdate { op: "+=", record:Box::new(left), value:Box::new(value), output_type: state.output_type })
});

parser!(update_merge(state) -> Node<'a> {
    let left = call!(state, mutating_record_reference);
    tag!(state, "<-");
    let value = call!(state, record);
    pos_result!(state, Node::RecordUpdate { op: "<-", record:Box::new(left), value:Box::new(value), output_type: state.output_type })
});

parser!(update_set(state) -> Node<'a> {
    let left = call!(state, mutating_record_reference);
    tag!(state, ":=");
    let value = alt!(state, [ none_value record record_set wrapped_record_set expression expression_set ]);
    pos_result!(state, Node::RecordUpdate { op: ":=", record:Box::new(left), value:Box::new(value), output_type: state.output_type })
});

parser!(update_remove(state) -> Node<'a> {
    let left = call!(state, mutating_record_reference);
    tag!(state, "-=");
    let value = alt!(state, [ expression expression_set hashtag ]);
    pos_result!(state, Node::RecordUpdate { op: "-=", record:Box::new(left), value:Box::new(value), output_type: state.output_type })
});

parser!(bind_update(state) -> Node<'a> {
    let result = alt!(state, [ update_add update_merge ]);
    result!(state, result)
});

parser!(commit_update(state) -> Node<'a> {
    let result = alt!(state, [ update_add update_merge update_set update_remove ]);
    result!(state, result)
});

parser!(output_equality(state) -> Node<'a> {
    let left = call!(state, variable);
    tag!(state, "=");
    let right = call!(state, record);
    pos_result!(state, Node::Equality { left:Box::new(left), right:Box::new(right) })
});

//--------------------------------------------------------------------
// Not
//--------------------------------------------------------------------

parser!(not_statement(state) -> Node<'a> {
    let item = alt!(state, [ not_form lookup_remote lookup_commit lookup multi_function_equality inequality record_function record equality attribute_access ]);
    result!(state, item)
});

parser!(not_form(state) -> Node<'a> {
    tag!(state, "not");
    tag!(state, "(");
    let items = many!(state, not_statement);
    tag!(state, ")");
    pos_result!(state, Node::Not(0, items))
});

//--------------------------------------------------------------------
// If
//--------------------------------------------------------------------

parser!(if_equality(state) -> Vec<Node<'a>> {
    let outputs = alt!(state, [ expression expression_set ]);
    tag!(state, "=");
    let items = match outputs.unwrap_pos() {
        Node::ExprSet(items) => items,
        any => vec![any],
    };
    result!(state, items)
});

parser!(else_only_branch(state) -> Node<'a> {
    tag!(state, "else");
    let result = alt!(state, [ expression expression_set ]);
    pos_result!(state, Node::IfBranch {sub_block_id:0, exclusive:true, body:vec![], result:Box::new(result)})
});

parser!(else_branch(state) -> Node<'a> {
    tag!(state, "else");
    let mut branch = call!(state, if_branch);
    if let Node::Pos(_, box Node::IfBranch { ref mut exclusive, .. }) = branch {
        *exclusive = true;
    } else {
        panic!("Invalid if branch");
    };
    result!(state, branch)
});

parser!(if_else_branch(state) -> Node<'a> {
    let result = alt!(state, [ if_branch else_branch else_only_branch ]);
    result!(state, result)
});

parser!(if_branch_statement(state) -> Node<'a> {
    let item = alt!(state, [ lookup_remote lookup_commit lookup multi_function_equality not_form inequality record_function record equality attribute_access ]);
    result!(state, item)
});

parser!(if_branch(state) -> Node<'a> {
    tag!(state, "if");
    let body = many!(state, if_branch_statement);
    tag!(state, "then");
    let result = alt!(state, [ expression expression_set ]);
    pos_result!(state, Node::IfBranch {sub_block_id:0, exclusive:false, body, result:Box::new(result)})
});

parser!(if_expression(state) -> Node<'a> {
    let outputs = opt!(state, if_equality);
    let start_branch = call!(state, if_branch);
    let other_branches = many!(state, if_else_branch);
    let exclusive = other_branches.iter().any(|b| {
        if let &Node::IfBranch {exclusive, ..} = b.unwrap_ref_pos() {
            exclusive
        } else {
            false
        }
    });
    let mut branches = vec![start_branch];
    branches.extend(other_branches);
    pos_result!(state, Node::If { sub_block_id:0, exclusive, outputs, branches })
});

//--------------------------------------------------------------------
// Sections
//--------------------------------------------------------------------

parser!(search_section_statement(state) -> Node<'a> {
    let item = alt!(state, [ not_form lookup_remote lookup_commit lookup multi_function_equality if_expression inequality
                             record_function record equality attribute_access ]);
    result!(state, item)
});

parser!(search_section(state) -> Node<'a> {
    tag!(state, "search");
    state.output_type = OutputType::Lookup;
    let items = many_1!(state, search_section_statement => EmptySearch);
    pos_result!(state, Node::Search(items))
});

parser!(bind_section_statement(state) -> Node<'a> {
    let item = alt!(state, [ lookup_remote lookup output_equality record bind_update ]);
    result!(state, item)
});

parser!(bind_section(state) -> Node<'a> {
    tag!(state, "bind");
    state.output_type = OutputType::Bind;
    let items = many_1!(state, bind_section_statement => EmptyUpdate);
    pos_result!(state, Node::Bind(items))
});

parser!(commit_section_statement(state) -> Node<'a> {
    let item = alt!(state, [ lookup_remote lookup output_equality record commit_update ]);
    result!(state, item)
});

parser!(commit_section(state) -> Node<'a> {
    tag!(state, "commit");
    state.output_type = OutputType::Commit;
    let items = many_1!(state, commit_section_statement => EmptyUpdate);
    pos_result!(state, Node::Commit(items))
});

parser!(project_section(state) -> Node<'a> {
    tag!(state, "project");
    tag!(state, "(");
    let items = many_1!(state, expression => EmptyUpdate);
    tag!(state, ")");
    pos_result!(state, Node::Project(items))
});

parser!(watch_section(state) -> Node<'a> {
    tag!(state, "watch");
    let watcher = match call!(state, identifier).unwrap_pos() {
        Node::Identifier(v) => v,
        _ => unreachable!(),
    };
    let items = many_1!(state, expression_set => EmptyUpdate);
    pos_result!(state, Node::Watch(watcher, items))
});

//--------------------------------------------------------------------
// Block
//--------------------------------------------------------------------

parser!(block_end(state) -> () {
    tag!(state, "end");
    result!(state, ())
});

parser!(block_update_section(state) -> Node<'a> {
    let update = alt!(state, [ bind_section commit_section project_section watch_section ]);
    result!(state, update)
});

parser!(block(state) -> Node<'a> {
    let mut errors = vec![];
    let s = search_section(state);
    let mut has_search = false;
    let search = match s {
        ParseResult::Ok(node) => {
            has_search = true;
            Some(node)
        },
        err @ ParseResult::Error(..) => {
            has_search = true;
            errors.push(err);
            None
        },
        _ => None,
    };
    let mut has_update = false;
    let update = match block_update_section(state) {
        ParseResult::Ok(node) => {
            has_update = true;
            Some(node)
        },
        err @ ParseResult::Error(..) => {
            has_update = true;
            errors.push(err);
            Some(Node::NoneValue)
        },
        _ => { errors.push(state.make_error(ParseError::MissingUpdate)); None },
    };
    if !has_search && !has_update {
        return state.fail(MatchType::Block);
    }
    match state.consume("end") {
        Err(_) => { errors.push(state.make_error(ParseError::MissingEnd)); }
        _ => {}
    }
    if errors.len() > 0 {
       state.consume_until(block_end);
    }
    pos_result!(state, Node::Block {code: state.input, errors, search:Box::new(search), update:Box::new(update.unwrap_or(Node::NoneValue))})
});

parser!(block_start(state) -> &'a str {
    let open = alt_tag!(state, [ "disabled" "search" "commit" "bind" "project" "watch" ]);
    result!(state, open)
});

parser!(embedded_blocks(state, file:&str) -> Node<'a> {
    let end = state.input.len();
    let mut blocks = vec![];
    while state.pos < end {
        state.mark("line");
        let has_start = opt!(state, block_start);
        match has_start {
            None => { state.pop(); state.consume_line(); }
            Some(v) => {
                state.backtrack();
                let block_pos = state.pos;
                let block_line = state.line;
                let block_ch = state.ch;
                while state.pos < end {
                    if let Some(_) = opt!(state, block_end) { break; }
                    state.consume_line();
                }
                let block_content = &state.input[block_pos..state.pos];
                let mut block_state = ParseState::new(block_content);
                block_state.line = block_line;
                block_state.ch = block_ch;
                if v == "disabled" {
                    blocks.push(Node::DisabledBlock(block_content));
                } else {
                    let result = block(&mut block_state);
                    match result {
                        ParseResult::Ok(block) => blocks.push(block),
                        _ => {}
                    }
                }
            },
        }
    }
    result!(state, Node::Doc { file:file.to_string(), blocks})
});
