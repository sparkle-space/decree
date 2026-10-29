# Upstream

decree was copied from four MIT-licensed repositories, reviewed at these tags on 2026-10-29:

| Upstream | Repository | Tag | Commit | Reviewed |
|---|---|---|---|---|
| commanded | https://github.com/commanded/commanded | v1.4.11 | `91d97bcdf00c7e58e8a8a43f499fc863cef17dee` | 2026-10-29 |
| eventstore | https://github.com/commanded/eventstore | v1.4.10 | `49ba9f971bd8be8b9bd77702f9eab4013d3fbbdd` | 2026-10-29 |
| commanded_eventstore_adapter | https://github.com/commanded/commanded-eventstore-adapter | v.1.4.2 | `7f348605c3539dd8c61e0163a5068793619ed7ec` | 2026-10-29 |
| commanded_ecto_projections | https://github.com/commanded/commanded-ecto-projections | v1.4.0 | `e2252b86c70b2f123ecc8ec9bea587ec4095dadc` | 2026-10-29 |

`PROVENANCE.tsv` names the upstream file and its SHA-256 for every copied file.

## Tailing window

The tailing window starts on 2026-10-29 and ends on **2027-01-29**.

Tailing is a monthly read of each upstream's changelog, issues and pull requests for defects in code decree kept. Each finding is marked:

- **take**: the defect is in kept code; port the fix by hand (no merge, no cherry-pick), with a test;
- **already-have**: decree already has the fix or never had the defect;
- **skip**: the change touches code decree did not keep (process managers, the in-memory adapter, the other serializers) or is not a defect.

After 2027-01-29 upstream is ignored.

`scripts/upstream-review.sh` lists, per upstream, the commits since the reviewed commit and the issues and pull requests updated since the reviewed date. It only reads (`gh`):

```bash
scripts/upstream-review.sh               # since the reviewed date
scripts/upstream-review.sh 2026-11-29    # since a later review
```
