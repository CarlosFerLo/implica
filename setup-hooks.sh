#!/bin/bash

# Script to set up Git hooks for the implica project

set -e

echo "🔧 Setting up Git hooks for implica..."

# Get the repository root directory
REPO_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || pwd)

# Set Git to use our custom hooks directory
git config core.hooksPath .githooks

echo "✅ Git hooks configured successfully!"
echo "📝 The pre-commit hook will now:"
echo "   - Format Rust code with 'cargo fmt'"
echo "   - Check Rust code with 'cargo clippy'"
echo "   - Format Python code with 'black'"
echo "   - Prevent commits if linting issues can't be auto-fixed"
echo ""
echo "💡 To bypass hooks temporarily (not recommended), use: git commit --no-verify"
