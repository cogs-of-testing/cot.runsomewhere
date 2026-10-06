# Releasing

Versions come from git tags (hatch-vcs). Pushing a `v*` tag runs
`.github/workflows/release.yml`, which tests, builds, smoke-tests the wheel
and publishes `cot-runsomewhere` to PyPI through trusted publishing.

## Once: trusted publisher

PyPI knows this repository as the publisher of `cot-runsomewhere`:

| field       | value              |
| ----------- | ------------------ |
| owner       | `cogs-of-testing`  |
| repository  | `cot.runsomewhere` |
| workflow    | `release.yml`      |
| environment | `pypi`             |

## Each release

1. Every change a user would notice adds a fragment to `changelog.d/`
   (`changelog.d/README.md` lists the types and the bump each causes).
2. Each push to `main` with fragments refreshes the pull request
   "Release vX.Y.Z" from `release/main`
   (`.github/workflows/release-proposal.yml`): the fragments rendered into
   `CHANGELOG.md` by towncrier, the version from
   `towncrier-fragments-zerover`. CI does not run on that pull request,
   because GitHub does not start workflows for one opened by `GITHUB_TOKEN`.
3. Merging it creates the tag `vX.Y.Z` and the GitHub release on the merge
   commit and starts `release.yml` on the tag
   (`.github/workflows/release-tag.yml`).

Pushing a `vX.Y.Z` tag by hand still releases that commit.

The proposal needs the repository setting "Allow GitHub Actions to create
and approve pull requests" (Settings, Actions, General).

The C speedups (`speedups/`, `cot-runsomewhere-speedups`) are not
published yet: they need a PyPI publisher of their own and a publish step in
`.github/workflows/speedups.yml`.
