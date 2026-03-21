//! Hand-written Cypher lexer.
//!
//! Produces a flat `Vec<Token>` from a Cypher string.  The parser consumes it
//! via a cursor.  All keywords are case-insensitive; identifiers are not.

use sparrowdb_common::{Error, Result};

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Keywords
    Match,
    Create,
    Return,
    Where,
    Not,
    And,
    Or,
    Order,
    By,
    Asc,
    Desc,
    Limit,
    Distinct,
    Optional,
    Union,
    Unwind,
    Delete,
    Detach,
    Set,
    Merge,
    Checkpoint,
    Optimize,
    Contains,
    StartsWith,
    EndsWith,
    Count,
    Null,
    True,
    False,
    As,
    With,
    Exists,

    // Punctuation
    LParen,    // (
    RParen,    // )
    LBrace,    // {
    RBrace,    // }
    LBracket,  // [
    RBracket,  // ]
    Colon,     // :
    Comma,     // ,
    Dot,       // .
    Arrow,     // ->
    LeftArrow, // <-
    Dash,      // -
    Pipe,      // |
    Star,      // *
    DotDot,    // ..

    // Operators
    Eq,  // =
    Neq, // <>
    Lt,  // <
    Le,  // <=
    Gt,  // >
    Ge,  // >=

    // Literals
    Integer(i64),
    Float(f64),
    Str(String),
    Param(String), // $name
    Ident(String),

    // Misc
    Eof,
    Semicolon,
}

/// Tokenize `input` into a `Vec<Token>`.
pub fn tokenize(input: &str) -> Result<Vec<Token>> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;
    let n = chars.len();

    while i < n {
        // Skip whitespace and comments
        if chars[i].is_ascii_whitespace() {
            i += 1;
            continue;
        }
        // Line comment: //
        if i + 1 < n && chars[i] == '/' && chars[i + 1] == '/' {
            while i < n && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }

        let tok = match chars[i] {
            '(' => {
                i += 1;
                Token::LParen
            }
            ')' => {
                i += 1;
                Token::RParen
            }
            '{' => {
                i += 1;
                Token::LBrace
            }
            '}' => {
                i += 1;
                Token::RBrace
            }
            '[' => {
                i += 1;
                Token::LBracket
            }
            ']' => {
                i += 1;
                Token::RBracket
            }
            ':' => {
                i += 1;
                Token::Colon
            }
            ',' => {
                i += 1;
                Token::Comma
            }
            '.' => {
                if i + 1 < n && chars[i + 1] == '.' {
                    i += 2;
                    Token::DotDot
                } else {
                    i += 1;
                    Token::Dot
                }
            }
            '|' => {
                i += 1;
                Token::Pipe
            }
            '*' => {
                i += 1;
                Token::Star
            }
            ';' => {
                i += 1;
                Token::Semicolon
            }
            '=' => {
                i += 1;
                Token::Eq
            }
            '<' => {
                if i + 1 < n && chars[i + 1] == '>' {
                    i += 2;
                    Token::Neq
                } else if i + 1 < n && chars[i + 1] == '=' {
                    i += 2;
                    Token::Le
                } else if i + 1 < n && chars[i + 1] == '-' {
                    i += 2;
                    Token::LeftArrow
                } else {
                    i += 1;
                    Token::Lt
                }
            }
            '>' => {
                if i + 1 < n && chars[i + 1] == '=' {
                    i += 2;
                    Token::Ge
                } else {
                    i += 1;
                    Token::Gt
                }
            }
            '-' => {
                if i + 1 < n && chars[i + 1] == '>' {
                    i += 2;
                    Token::Arrow
                } else {
                    i += 1;
                    Token::Dash
                }
            }
            '$' => {
                i += 1;
                let start = i;
                while i < n && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                if i == start {
                    return Err(Error::InvalidArgument(
                        "empty parameter name after $".into(),
                    ));
                }
                Token::Param(chars[start..i].iter().collect())
            }
            '"' | '\'' => {
                let quote = chars[i];
                i += 1;
                let mut s = String::new();
                while i < n && chars[i] != quote {
                    if chars[i] == '\\' && i + 1 < n {
                        i += 1;
                        match chars[i] {
                            'n' => s.push('\n'),
                            't' => s.push('\t'),
                            'r' => s.push('\r'),
                            '\\' => s.push('\\'),
                            '"' => s.push('"'),
                            '\'' => s.push('\''),
                            c => {
                                s.push('\\');
                                s.push(c);
                            }
                        }
                    } else {
                        s.push(chars[i]);
                    }
                    i += 1;
                }
                if i >= n {
                    return Err(Error::InvalidArgument("unterminated string literal".into()));
                }
                i += 1; // closing quote
                Token::Str(s)
            }
            c if c.is_ascii_digit() => {
                let start = i;
                while i < n && chars[i].is_ascii_digit() {
                    i += 1;
                }
                // Only treat `1.x` as a float when the char after '.' is a digit,
                // not when it's another '.' (which would form a `..` DotDot token).
                let is_float = i < n && chars[i] == '.' && i + 1 < n && chars[i + 1] != '.';
                if is_float {
                    i += 1;
                    while i < n && chars[i].is_ascii_digit() {
                        i += 1;
                    }
                    let s: String = chars[start..i].iter().collect();
                    let f: f64 = s.parse().map_err(|_| {
                        Error::InvalidArgument(format!("invalid float literal: {s}"))
                    })?;
                    Token::Float(f)
                } else {
                    let s: String = chars[start..i].iter().collect();
                    let v: i64 = s.parse().map_err(|_| {
                        Error::InvalidArgument(format!("invalid integer literal: {s}"))
                    })?;
                    Token::Integer(v)
                }
            }
            c if c.is_alphabetic() || c == '_' => {
                let start = i;
                while i < n && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                let word: String = chars[start..i].iter().collect();
                keyword_or_ident(word)
            }
            c => {
                return Err(Error::InvalidArgument(format!(
                    "unexpected character: {:?}",
                    c
                )));
            }
        };
        tokens.push(tok);
    }
    tokens.push(Token::Eof);
    Ok(tokens)
}

fn keyword_or_ident(word: String) -> Token {
    match word.to_uppercase().as_str() {
        "MATCH" => Token::Match,
        "CREATE" => Token::Create,
        "RETURN" => Token::Return,
        "WHERE" => Token::Where,
        "NOT" => Token::Not,
        "AND" => Token::And,
        "OR" => Token::Or,
        "ORDER" => Token::Order,
        "BY" => Token::By,
        "ASC" => Token::Asc,
        "DESC" => Token::Desc,
        "LIMIT" => Token::Limit,
        "DISTINCT" => Token::Distinct,
        "OPTIONAL" => Token::Optional,
        "UNION" => Token::Union,
        "UNWIND" => Token::Unwind,
        "DELETE" => Token::Delete,
        "DETACH" => Token::Detach,
        "SET" => Token::Set,
        "MERGE" => Token::Merge,
        "CHECKPOINT" => Token::Checkpoint,
        "OPTIMIZE" => Token::Optimize,
        "CONTAINS" => Token::Contains,
        "STARTS" => Token::StartsWith, // STARTS WITH
        "ENDS" => Token::EndsWith,     // ENDS WITH
        "COUNT" => Token::Count,
        "NULL" => Token::Null,
        "TRUE" => Token::True,
        "FALSE" => Token::False,
        "AS" => Token::As,
        "WITH" => Token::With,
        "EXISTS" => Token::Exists,
        _ => Token::Ident(word),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_simple_match() {
        let toks = tokenize("MATCH (n:Person) RETURN n.name").unwrap();
        assert!(toks.contains(&Token::Match));
        assert!(toks.contains(&Token::Return));
        assert!(toks.contains(&Token::Dot));
    }

    #[test]
    fn tokenize_string_literal() {
        let toks = tokenize("\"Alice\"").unwrap();
        assert_eq!(toks[0], Token::Str("Alice".into()));
    }

    #[test]
    fn tokenize_arrow() {
        let toks = tokenize("->").unwrap();
        assert_eq!(toks[0], Token::Arrow);
    }

    #[test]
    fn tokenize_param() {
        let toks = tokenize("$name").unwrap();
        assert_eq!(toks[0], Token::Param("name".into()));
    }
}
