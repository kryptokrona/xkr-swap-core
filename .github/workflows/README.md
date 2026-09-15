# Workflow documentation

## `ci.yml`

Defines the Continuous Integration workflow for merging into the `master` branch.

## Releases

Releases are driven entirely by **pushing a git tag**. There is no separate
"create a release" workflow to run first — [build-release-binaries.yml](build-release-binaries.yml)
does everything on the tag push.

### Making a new release

1. Bump the version in the crate manifests (`swap/Cargo.toml`, `swap-asb/Cargo.toml`, …)
   and move the `Unreleased` section of the Changelog to the new version.
2. Commit and push to `master`.
3. Tag the commit and push the tag:

   ```sh
   git tag v1.2.3
   git push origin v1.2.3
   ```

That tag push triggers [build-release-binaries.yml](build-release-binaries.yml), which:

- **Creates the GitHub release** for the tag (idempotently — if one already exists
  for the tag, it reuses it).
- **Marks it as a pre-release** when the tag name contains a pre-release identifier
  (`alpha`, `beta`, `rc`, `pre`, `preview`, `dev`, `snapshot`, `nightly`, `test`),
  e.g. `v1.2.3-beta.1` or `test-<sha>`. Otherwise it is a full release.
- **Builds** `swap`, `asb`, `asb-controller`, `rendezvous-node` and `orchestrator`
  in release mode for Linux x64, macOS arm64, macOS x64 and Windows x64.
- **GPG-signs** each archive and attaches the archive + `.asc` signature to the release.
- **Builds and pushes the Docker images** (`asb`, `asb-controller`) to ghcr, except
  for `test-*` tags. Pre-releases are published **without** the moving `:latest` tag.

To release from an arbitrary commit (e.g. a test build), just tag that commit —
prefix the tag with `test-` to get a pre-release that skips the Docker publish.

You can also still create a release manually through the GitHub web interface; the
build workflow keys off the tag, so attaching binaries works the same way.
