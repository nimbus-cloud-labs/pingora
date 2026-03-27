# Repository Contribution Instructions

## Scope
- This repository is a Rust workspace made of library crates, examples, benches, and tests.
- Prefer instructions that match the current Pingora codebase and CI workflows over generic policies copied from other projects.

## Formatting and verification
- When changing Rust source files, `Cargo.toml`, or `Cargo.lock`, run `cargo fmt --all`.
- Before wrapping up Rust changes, run the narrowest relevant checks you can justify locally.
- For broad workspace validation, follow the CI shape in `.github/workflows/build.yml`:
  - `cargo test --verbose --lib --bins --tests --no-fail-fast`
  - `cargo test --verbose --doc`
  - `cargo clippy --all-targets --all -- --allow=unknown-lints --deny=warnings`
- If you cannot run a check because of time, toolchain, or system dependencies, say so explicitly.

## Cargo manifests
- Keep each `Cargo.toml` consistent with the surrounding file instead of forcing a new manifest style.
- Preserve the existing ordering and formatting conventions already used in the manifest you are editing.
- When adding dependencies, prefer the least disruptive change and keep related entries grouped coherently.
- Do not perform repo-wide manifest style rewrites unless the task explicitly asks for them.

## Rust guidelines
- Treat `rust-guidelines.txt` as a useful reference for public API design and documentation, not as a reason to rewrite unrelated code.
- Add `// SAFETY:` comments for every new or modified `unsafe` block.
- Follow the crate's existing lint style unless the task explicitly includes lint cleanup.
- Do not replace existing `#[allow(...)]` usage wholesale unless there is a targeted reason and the change is validated.
- Be careful with allocator changes. This repository already uses explicit allocators in some examples and benches; do not standardize allocator choice without a task-specific reason.

## Documentation
- Write documentation in English.
- Update user-facing documentation when behavior, public APIs, examples, or setup steps change.
- Keep public Rust docs concise and idiomatic. For public items, include canonical sections such as `# Examples`, `# Errors`, `# Panics`, or `# Safety` when they apply.
- Prefer updating the closest existing documentation file instead of creating duplicate docs.

## Repository-specific notes
- Pingora is primarily a library workspace. Be conservative about adding application-specific conventions that do not fit library crates, examples, or benches.

## Change proposals
- When asked to analyze the codebase and suggest work, keep the output practical:
  - short summary of the issue or improvement
  - concrete implementation steps
  - enough file or symbol context for someone to find the code quickly
