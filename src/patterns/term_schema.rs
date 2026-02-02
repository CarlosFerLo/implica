use std::fmt::Display;

use error_stack::ResultExt;

use crate::ctx;
use crate::errors::{ImplicaError, ImplicaResult};
use crate::utils::validate_variable_name;

#[derive(Clone, Debug, PartialEq)]
pub enum TermPattern {
    Wildcard,
    Variable(String),
    Application {
        function: Box<TermPattern>,
        argument: Box<TermPattern>,
    },
    Constant {
        name: String,
        args: Vec<String>,
    },
}

#[derive(Clone, Debug)]
pub struct TermSchema {
    pub pattern: String,
    pub compiled: TermPattern,
}

impl Display for TermSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TermSchema('{}')", self.pattern)
    }
}

impl TermSchema {
    pub fn new(pattern: String) -> ImplicaResult<Self> {
        let compiled = Self::parse_pattern(&pattern).attach(ctx!("term schema - new"))?;

        Ok(TermSchema { pattern, compiled })
    }

    fn parse_pattern(input: &str) -> ImplicaResult<TermPattern> {
        let trimmed = input.trim();

        // Check for wildcard
        if trimmed == "*" {
            return Ok(TermPattern::Wildcard);
        }

        // Check if it contains spaces (application)
        // For left associativity, we need to find the LAST space at depth 0 (not inside parentheses)
        if let Some(space_pos) = Self::find_last_space_at_depth_zero(trimmed) {
            // Split at the last space for left associativity
            // "f s t" becomes "(f s)" and "t"
            let left_str = trimmed[..space_pos].trim();
            let right_str = trimmed[space_pos + 1..].trim();

            if left_str.is_empty() || right_str.is_empty() {
                return Err(ImplicaError::InvalidPattern {
                    pattern: input.to_string(),
                    reason: "Invalid application pattern: empty left or right side".to_string(),
                }
                .into());
            }

            // Recursively parse left and right
            let function = Box::new(
                Self::parse_pattern(left_str).attach(ctx!("term schema - parse pattern"))?,
            );
            let argument = Box::new(
                Self::parse_pattern(right_str).attach(ctx!("term schema - parse pattern"))?,
            );

            return Ok(TermPattern::Application { function, argument });
        }

        // Check for constant pattern: @ConstantName(Arg1, Arg2, ...)
        if trimmed.starts_with('@') {
            return Self::parse_constant_pattern(trimmed)
                .attach(ctx!("term schema - parse pattern"));
        }

        // Otherwise, it's a variable
        if trimmed.is_empty() {
            return Err(ImplicaError::InvalidPattern {
                pattern: input.to_string(),
                reason: "Invalid pattern: empty string".to_string(),
            }
            .into());
        }

        validate_variable_name(trimmed).attach(ctx!("term schema - parse pattern"))?;
        Ok(TermPattern::Variable(trimmed.to_string()))
    }

    fn find_last_space_at_depth_zero(input: &str) -> Option<usize> {
        let mut paren_depth = 0;
        let mut last_space_pos = None;

        for (i, ch) in input.char_indices() {
            match ch {
                '(' => paren_depth += 1,
                ')' => paren_depth -= 1,
                ' ' if paren_depth == 0 => last_space_pos = Some(i),
                _ => {}
            }
        }

        last_space_pos
    }

    fn parse_constant_pattern(input: &str) -> ImplicaResult<TermPattern> {
        // Input should be like: @K(A, B) or @S(A, A->B, C)
        if !input.starts_with('@') {
            return Err(ImplicaError::InvalidPattern {
                pattern: input.to_string(),
                reason: "Constant pattern must start with '@'".to_string(),
            }
            .into());
        }

        // Find the opening parenthesis
        let paren_start = input
            .find('(')
            .ok_or_else(|| ImplicaError::InvalidPattern {
                pattern: input.to_string(),
                reason: "Constant pattern must have parentheses with type arguments".to_string(),
            })?;

        // Extract constant name (everything between @ and '(')
        let name = input[1..paren_start].trim().to_string();

        if name.is_empty() {
            return Err(ImplicaError::InvalidPattern {
                pattern: input.to_string(),
                reason: "Constant name cannot be empty".to_string(),
            }
            .into());
        }

        // Find the matching closing parenthesis
        let paren_end = Self::find_matching_closing_paren(input, paren_start)
            .attach(ctx!("term pattern - parse constant pattern"))?;

        // Verify that the constant pattern ends at the closing parenthesis (no trailing content)
        if paren_end != input.len() - 1 {
            return Err(ImplicaError::InvalidPattern {
                pattern: input.to_string(),
                reason: format!(
                    "Constant pattern has unexpected content after closing parenthesis at position {}",
                    paren_end
                ),
            }.into());
        }

        // Extract the arguments string (everything between '(' and ')')
        let args_str = input[paren_start + 1..paren_end].trim();

        // Parse the arguments - split by comma, but be careful with nested structures
        let args = if args_str.is_empty() {
            Vec::new()
        } else {
            Self::split_type_arguments(args_str)
                .attach(ctx!("term pattern - parse constant pattern"))?
        };

        Ok(TermPattern::Constant { name, args })
    }

    fn find_matching_closing_paren(input: &str, open_pos: usize) -> ImplicaResult<usize> {
        let mut depth = 0;

        for (i, ch) in input[open_pos..].char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        return Ok(open_pos + i);
                    }
                }
                _ => {}
            }
        }

        Err(ImplicaError::InvalidPattern {
            pattern: input.to_string(),
            reason: "Constant pattern has unmatched opening parenthesis".to_string(),
        }
        .into())
    }

    fn split_type_arguments(args_str: &str) -> ImplicaResult<Vec<String>> {
        let mut args = Vec::new();
        let mut current_arg = String::new();
        let mut paren_depth = 0;

        for ch in args_str.chars() {
            match ch {
                '(' => {
                    paren_depth += 1;
                    current_arg.push(ch);
                }
                ')' => {
                    paren_depth -= 1;
                    current_arg.push(ch);
                }
                ',' if paren_depth == 0 => {
                    // This comma is a separator at the top level
                    let trimmed = current_arg.trim().to_string();
                    if !trimmed.is_empty() {
                        args.push(trimmed);
                    }
                    current_arg.clear();
                }
                _ => {
                    current_arg.push(ch);
                }
            }
        }

        // Don't forget the last argument
        let trimmed = current_arg.trim().to_string();
        if !trimmed.is_empty() {
            args.push(trimmed);
        }

        if paren_depth != 0 {
            return Err(ImplicaError::InvalidPattern {
                pattern: args_str.to_string(),
                reason: "Mismatched parentheses in constant type arguments".to_string(),
            }
            .into());
        }

        Ok(args)
    }
}
