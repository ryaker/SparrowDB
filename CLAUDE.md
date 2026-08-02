# SparrowDB — build and test environment

Machine-specific facts that will otherwise cost you a wrong result. Verified 2026-08-01.

## Isolate CARGO_TARGET_DIR when worktrees build concurrently

Every worktree shares `CARGO_TARGET_DIR=/Volumes/Dev/cargo-target`. With more than one
worktree or agent building at the same time, `cargo test` can run a binary compiled from
**another worktree's source**.

```bash
export CARGO_TARGET_DIR=/Volumes/Dev/target-<pr-number>
```

Tells that a result is contaminated:
- test names in the output that don't exist in your file
- an ignored-count that disagrees with `grep -c '#\[ignore'`
- a pass where you predicted a failure

Treat any suite result gathered while sibling agents were building as unverified.

## `cargo test --workspace` does not compile here

`sparrowdb-ruby` → `magnus`/`rb_sys` fails against the installed Ruby (~79 errors:
`RUBY_T_MOVED` not found, `dcompact` missing). Always:

```bash
cargo test --workspace --exclude sparrowdb-ruby --no-fail-fast
```

CI is unaffected — its Ruby job pins 3.3 via `ruby/setup-ruby@v1`.

## Always `--no-fail-fast`

`cargo test` stops after a failing test **binary**, so every later binary silently never
runs. A truncated failure list looks identical to a short one. `cargo clippy` likewise
stops at the first failing crate — use `--keep-going` to enumerate all lints in one pass.

## The suite is slow

`spa_222_csr_lazy_load` alone runs 5+ minutes; a full run exceeds 60. It is not hung.
CI runs take ~85 minutes.

## cargo binary

`~/.cargo/bin/cargo` is a broken symlink. Use:

```
/Users/ryaker/.rustup/toolchains/stable-aarch64-apple-darwin/bin/cargo
```

## Two testing rules this repo learned the hard way

1. **Derive expected values from the fixture by hand — never capture program output.**
   Asserting what the code currently returns freezes bugs in as expected behaviour. Issue
   #421 (silent variable-length path truncation) was found precisely by hand-deriving an
   expected result instead of recording the observed one.

2. **Confirm a regression test fails against the pre-fix commit.** `regression_406.rs`
   shipped as the guard for #406 and passes against the unfixed code — it could never
   reach the failing state, so it guarded nothing. Check out the fix's parent, run the
   test, and verify it fails with the reported symptom.
