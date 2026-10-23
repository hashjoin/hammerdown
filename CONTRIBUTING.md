# Contributing

Thanks for helping. hammerdown is meant to be the boring, trustworthy core
that many ops teams fork; contributions that keep it boring and trustworthy
are the most welcome.

## Good first contributions

- A backend for a system you run (Patroni, Redis Cluster, MongoDB, Kafka,
  Elasticsearch, Kubernetes StatefulSets ...). Keep client libraries inside
  the backend package so the core stays standard-library only.
- Guards you wish you had at 3 a.m. (replication lag, backup in progress,
  open incident, change freeze).
- Filters (labels, versions ranges) and report views.
- War stories as issues: "this is how a roll went wrong for us" makes the
  defaults better for everyone.

## How to propose a change

1. **Small fix?** Open a PR directly.
2. **New feature, backend or changed default?** Open an issue first (there
   are templates for features and backends) so we can agree on the shape
   before you write code.
3. **Questions, "how do I run this against X"?** Use Discussions.
4. Look for issues labelled `good first issue` or `help wanted`.

Every PR runs lint, race tests, a vulnerability scan and the 11,000-node
demo in CI. Add yourself to `AUTHORS` in your first PR.

## Ground rules

- `make fmt lint test` must pass. CI runs the same golangci-lint version
  (pinned in the Makefile and `.github/workflows/ci.yml`). Install that
  exact version without touching your Go toolchain:
  `curl -sSfL https://golangci-lint.run/install.sh | sh -s -- -b "$(go env GOPATH)/bin" v2.15.0`
  (make sure `$(go env GOPATH)/bin` is on your PATH).
- Formatting is gofumpt + goimports, enforced by the linter, so editor
  settings don't matter: run `make fmt` before committing. `.editorconfig`
  covers the non-Go files.
- Read `CLAUDE.md` - its invariants apply to human PRs too.
- No new dependencies in `fleet/ filter/ roll/ lock/ report/ backend/ cmd/`.
- Changing a default (workers, fail-fast thresholds, dry-run) needs a reason
  in the PR description.
- Using Claude or another assistant is fine; you own the result.

## Security

Please report vulnerabilities privately; see [SECURITY.md](SECURITY.md).
This project follows the [code of conduct](CODE_OF_CONDUCT.md).

## Releasing

Tags are `vMAJOR.MINOR.PATCH`. Pushing a tag is the whole release:

```sh
git tag v0.1.0 && git push origin v0.1.0
```

`.github/workflows/release.yml` runs the tests, cross-compiles macOS and
Linux binaries (amd64 and arm64), and publishes a GitHub release with
checksums and auto-generated notes. Don't create the release by hand first.
