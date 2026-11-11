# Releasing

A release is one run of the `Release` workflow. It publishes the package to PyPI from CI; nobody publishes from a laptop.

1. In a pull request, set the new version in `pyproject.toml` and `rask_sdk/_version.py`, and in the test of the
   `User-Agent` in `tests/unit/test_client.py`, which names it. Merge it.
2. Start the workflow on the default branch, with "Run workflow" on the Actions tab or from a terminal:

   ```sh
   gh workflow run release.yml
   ```

3. The workflow runs the CI checks, builds the sdist and the wheel, publishes `rask-sdk` to PyPI, then tags the commit
   `v` plus the version and makes the GitHub Release.

The workflow refuses to run from any other branch, and refuses a version that already has its tag. The tag is made
last, so a version with a tag is a finished release. PyPI never takes the same file name twice, even after a release is
deleted: to fix a version, release the next.

If a run stops after PyPI took one of the files or both, run it again on the same commit: "Re-run failed jobs" on that
run, or a new run if nothing was merged since. It builds the same files, skips those PyPI already has, uploads the
rest, then tags the commit and makes the GitHub Release.

A file that PyPI has with other bytes stops the run before it is uploaded: the commit or the build tools are no longer
those of the first run. If PyPI then has both files, only the tag is missing, and it is made by hand:
`gh release create v<version> --target <the commit of the first run> --generate-notes`, with `--prerelease` for a
pre-release version. If it has one, release the next version.

## Set up once

PyPI trusts the workflow itself (trusted publishing), so this repository holds no PyPI token.

- On pypi.org, the account or organization that is to own `rask-sdk` names the publisher. The project does not exist
  before its first release, so this is a pending publisher (Publishing, "Add a new pending publisher"): PyPI project
  name `rask-sdk`, owner `braskai`, repository `rask-sdk-python`, workflow `release.yml`, environment `pypi`. The first
  release then creates the project.
- In this repository, the environment `pypi` takes only the default branch (Settings → Environments → `pypi` →
  Deployment branches and tags: selected branches and tags, one rule, the default branch). That is what keeps a run
  on any other branch from publishing. If the default branch is renamed, change the rule with it.
