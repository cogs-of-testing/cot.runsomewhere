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

1. Update `CHANGELOG.md` on `main`, if the project keeps one.

2. Tag the commit on `main` and push the tag:

   ```
   git tag vX.Y.Z
   git push origin vX.Y.Z
   ```

The C speedups (`speedups/`, `cot-runsomewhere-speedups`) are not
published yet: they need a PyPI publisher of their own and a publish step in
`.github/workflows/speedups.yml`.
