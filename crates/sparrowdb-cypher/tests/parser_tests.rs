//! Parser tests — RED phase: these must fail before Phase 4a implementation.

use sparrowdb_cypher::ast::Statement;
use sparrowdb_cypher::parser::parse;

// ── CREATE node ──────────────────────────────────────────────────────────────

#[test]
fn parse_create_single_node_no_props() {
    let stmt = parse("CREATE (n:Person)").expect("must parse");
    assert!(matches!(stmt, Statement::Create(_)));
}

#[test]
fn parse_create_single_node_with_props() {
    let stmt = parse("CREATE (n:Person {name: \"Alice\", age: 30})").expect("must parse");
    if let Statement::Create(c) = stmt {
        assert_eq!(c.nodes.len(), 1);
        let node = &c.nodes[0];
        assert_eq!(node.labels[0], "Person");
        assert_eq!(node.props.len(), 2);
    } else {
        panic!("expected Create statement");
    }
}

#[test]
fn parse_create_multi_label_node() {
    let stmt = parse("CREATE (n:Person:Employee {name: \"Bob\"})").expect("must parse");
    if let Statement::Create(c) = stmt {
        assert_eq!(c.nodes[0].labels, vec!["Person", "Employee"]);
    } else {
        panic!("expected Create statement");
    }
}

// ── CREATE edge ──────────────────────────────────────────────────────────────

#[test]
fn parse_match_create_edge() {
    let stmt = parse(
        "MATCH (a:Person {name: \"Alice\"}), (b:Person {name: \"Bob\"}) \
         CREATE (a)-[:KNOWS]->(b)",
    )
    .expect("must parse");
    // This is a compound statement: a Match + Create
    assert!(matches!(stmt, Statement::MatchCreate(_)));
}

// ── MATCH + RETURN ────────────────────────────────────────────────────────────

#[test]
fn parse_match_node_return() {
    let stmt = parse("MATCH (n:Person {name: \"Alice\"}) RETURN n.name").expect("must parse");
    if let Statement::Match(m) = stmt {
        assert_eq!(m.pattern.len(), 1);
        let p = &m.pattern[0];
        assert_eq!(p.nodes[0].labels[0], "Person");
        assert!(!m.return_clause.items.is_empty());
    } else {
        panic!("expected Match statement");
    }
}

#[test]
fn parse_match_1hop_friends() {
    // UC-1 core query: Alice's friends
    let stmt = parse("MATCH (a:Person {name: \"Alice\"})-[:KNOWS]->(f:Person) RETURN f.name")
        .expect("must parse");
    if let Statement::Match(m) = stmt {
        assert_eq!(m.pattern.len(), 1);
        let p = &m.pattern[0];
        assert_eq!(p.nodes.len(), 2);
        assert_eq!(p.rels.len(), 1);
        assert_eq!(p.rels[0].rel_type, "KNOWS");
    } else {
        panic!("expected Match");
    }
}

#[test]
fn parse_match_2hop_fof() {
    // UC-1 2-hop friend-of-friend
    let stmt = parse(
        "MATCH (a:Person {name: \"Alice\"})-[:KNOWS]->()-[:KNOWS]->(fof:Person) \
         WHERE NOT (a)-[:KNOWS]->(fof) \
         RETURN DISTINCT fof.name",
    )
    .expect("must parse");
    if let Statement::Match(m) = stmt {
        assert!(m.distinct);
        assert_eq!(m.pattern[0].rels.len(), 2);
    } else {
        panic!("expected Match");
    }
}

#[test]
fn parse_match_where_contains() {
    // UC-3 KMS query with CONTAINS
    let stmt =
        parse("MATCH (k:Knowledge) WHERE k.content CONTAINS $query RETURN k").expect("must parse");
    if let Statement::Match(m) = stmt {
        assert!(m.where_clause.is_some());
    } else {
        panic!("expected Match");
    }
}

#[test]
fn parse_match_with_order_by_limit() {
    let stmt = parse("MATCH (k:Knowledge) RETURN k ORDER BY k.confidence DESC LIMIT 20")
        .expect("must parse");
    if let Statement::Match(m) = stmt {
        assert!(!m.order_by.is_empty());
        assert_eq!(m.limit, Some(20));
    } else {
        panic!("expected Match");
    }
}

#[test]
fn parse_match_mutual_friends() {
    // UC-1 mutual friends: Alice and Bob
    let stmt = parse(
        "MATCH (a:Person {name: \"Alice\"})-[:KNOWS]->(m:Person)<-[:KNOWS]-(b:Person {name: \"Bob\"}) \
         RETURN m.name",
    )
    .expect("must parse");
    assert!(matches!(stmt, Statement::Match(_)));
}

// ── CHECKPOINT / OPTIMIZE stubs ───────────────────────────────────────────────

#[test]
fn parse_checkpoint() {
    let stmt = parse("CHECKPOINT").expect("must parse");
    assert!(matches!(stmt, Statement::Checkpoint));
}

#[test]
fn parse_optimize() {
    let stmt = parse("OPTIMIZE").expect("must parse");
    assert!(matches!(stmt, Statement::Optimize));
}

// ── Must-fail cases ───────────────────────────────────────────────────────────

#[test]
fn parse_optional_match_ok() {
    // OPTIONAL MATCH standalone is now supported (SPA-131).
    let stmt = parse("OPTIONAL MATCH (n:Person) RETURN n").expect("OPTIONAL MATCH must parse");
    assert!(matches!(stmt, sparrowdb_cypher::ast::Statement::OptionalMatch(_)));
}

#[test]
fn parse_optional_match_missing_return_fails() {
    assert!(parse("OPTIONAL MATCH (n:Person)").is_err());
}

#[test]
fn parse_union_ok() {
    // UNION is now supported (SPA-132).
    let stmt = parse("MATCH (n:Person) RETURN n.name UNION MATCH (m:Person) RETURN m.name")
        .expect("UNION must parse");
    assert!(matches!(stmt, sparrowdb_cypher::ast::Statement::Union(_)));
}

#[test]
fn parse_unwind_list_literal() {
    // Phase 8: UNWIND [1,2,3] AS x RETURN x is now supported.
    let stmt = parse("UNWIND [1,2,3] AS x RETURN x").expect("UNWIND must parse");
    assert!(
        matches!(stmt, sparrowdb_cypher::ast::Statement::Unwind(_)),
        "expected Unwind statement"
    );
}

#[test]
fn parse_unwind_empty_list() {
    let stmt = parse("UNWIND [] AS x RETURN x").expect("UNWIND [] must parse");
    assert!(matches!(stmt, sparrowdb_cypher::ast::Statement::Unwind(_)));
}

#[test]
fn parse_unwind_string_list() {
    let stmt = parse("UNWIND ['a', 'b'] AS s RETURN s").expect("UNWIND string list must parse");
    assert!(matches!(stmt, sparrowdb_cypher::ast::Statement::Unwind(_)));
}

#[test]
fn parse_unwind_param() {
    let stmt = parse("UNWIND $items AS item RETURN item").expect("UNWIND $param must parse");
    assert!(matches!(stmt, sparrowdb_cypher::ast::Statement::Unwind(_)));
}

#[test]
fn parse_detach_delete_rejected() {
    assert!(parse("MATCH (n) DETACH DELETE n").is_err());
}

#[test]
fn parse_variable_length_path_supported() {
    // Variable-length paths are now supported (SPA-168+).
    // Verify each syntax variant parses without error.
    assert!(parse("MATCH (a:Person)-[:KNOWS*]->(b:Person) RETURN b.name").is_ok(), "[:R*] must parse");
    assert!(parse("MATCH (a:Person)-[:KNOWS*2]->(b:Person) RETURN b.name").is_ok(), "[:R*N] must parse");
    assert!(parse("MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN b.name").is_ok(), "[:R*M..N] must parse");
    assert!(parse("MATCH (a:Person)-[:KNOWS*..5]->(b:Person) RETURN b.name").is_ok(), "[:R*..N] must parse");
}

#[test]
fn parse_empty_input_rejected() {
    assert!(parse("").is_err());
}

#[test]
fn parse_unknown_keyword_rejected() {
    assert!(parse("FROBNICATE (n:Person) RETURN n").is_err());
}
