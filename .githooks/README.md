---
noteId: "cde14d20bbfd11f0bde2b977e9ee83f3"
tags: []
---

# Git Hooks for Implica

This directory contains Git hooks that automatically enforce code quality standards.

## Setup

Run the setup script from the project root:

```bash
./setup-hooks.sh
```

This configures Git to use the hooks in this directory.

## Pre-commit Hook

The `pre-commit` hook runs automatically before each commit and:

1. **Rust Code**:

   - Formats code with `cargo fmt`
   - Checks code with `cargo clippy --all-targets --all-features -- -D warnings`
   - Auto-fixes formatting issues
   - Fails if clippy finds errors that can't be auto-fixed

2. **Python Code**:
   - Formats code with `black`
   - Auto-fixes formatting issues
   - Installs `black` automatically if not present

## Behavior

- ✅ **Auto-formatting**: The hook automatically formats your code before commit
- ✅ **Staged files updated**: Formatted files are automatically re-staged
- ❌ **Blocks bad commits**: Commits are blocked if linting errors can't be fixed
- 🎨 **Color output**: Clear visual feedback on what's happening

## Bypassing Hooks

In rare cases where you need to bypass the hooks (not recommended):

```bash
git commit --no-verify
```

**Warning**: Bypassing hooks may cause CI failures in GitHub Actions.

## Benefits

- Ensures consistent code style across the project
- Catches common errors before they reach CI
- Makes the GitHub Actions lint check always pass
- Reduces code review friction

## Troubleshooting

### Hook not running

Check if Git is configured correctly:

```bash
git config core.hooksPath
# Should output: .githooks
```

Re-run setup if needed:

```bash
./setup-hooks.sh
```

### Hook permissions

Ensure the hook is executable:

```bash
chmod +x .githooks/pre-commit
```

### Clippy errors

If clippy finds errors that can't be auto-fixed, you'll need to fix them manually:

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

Fix the reported issues, then try committing again.
