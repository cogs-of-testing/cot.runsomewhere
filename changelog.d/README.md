# Changelog fragments

Every change a user of the package would notice adds one fragment here.
Each push to `main` renders them into a release proposal; see
`docs/releasing.md`.

Name a fragment `<issue or PR number>.<type>.md`, or `+<slug>.<type>.md`
when there is no number. The type sets the section and the version bump
(`towncrier-fragments-zerover`):

| type          | section          | bump on 0.x | bump from 1.0 |
| ------------- | ---------------- | ----------- | ------------- |
| `major`       | Major Changes    | 1.0.0       | major         |
| `breaking`    | Breaking Changes | minor       | major         |
| `removal`     | Removed          | minor       | major         |
| `deprecation` | Deprecated       | minor       | minor         |
| `feature`     | Added            | minor       | minor         |
| `bugfix`      | Fixed            | patch       | patch         |
| `doc`         | Documentation    | patch       | patch         |
| `misc`        | Miscellaneous    | patch       | patch         |
