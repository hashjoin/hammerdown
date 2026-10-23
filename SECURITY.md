# Security policy

hammerdown runs with the operator's credentials against production fleets,
so we take security reports seriously.

## Reporting a vulnerability

Please **do not open a public issue**. Report privately through GitHub:
[Security → Report a vulnerability](https://github.com/hashjoin/hammerdown/security/advisories/new).

Include what you found, how to reproduce it (the `sim` backend is ideal),
and the impact you expect. You'll get an acknowledgement within a few days
and we'll agree on a disclosure timeline together.

## Scope

In scope: the core packages, the bundled `sim` and `shell` backends, the
CLI, and the release binaries.

Out of scope: commands you pass to the `shell` backend (they run with your
privileges by design), and third-party backends living outside this repo.

## Supported versions

Fixes land on `main` and in the next release. Until 1.0, only the latest
release is supported.
