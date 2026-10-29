#!/usr/bin/env bash
# Lists upstream activity since decree's review (UPSTREAM.md): per upstream, the
# commits after the reviewed commit and the issues and pull requests updated since the
# given date. Read-only: it only queries GitHub through `gh`.
#
#   scripts/upstream-review.sh [YYYY-MM-DD]    (default: the reviewed date, 2026-10-29)

set -euo pipefail

since="${1:-2026-10-29}"

# repository  reviewed-commit
upstreams=(
  "commanded/commanded 91d97bcdf00c7e58e8a8a43f499fc863cef17dee"
  "commanded/eventstore 49ba9f971bd8be8b9bd77702f9eab4013d3fbbdd"
  "commanded/commanded-eventstore-adapter 7f348605c3539dd8c61e0163a5068793619ed7ec"
  "commanded/commanded-ecto-projections e2252b86c70b2f123ecc8ec9bea587ec4095dadc"
)

for entry in "${upstreams[@]}"; do
  read -r repo reviewed <<<"$entry"
  default_branch="$(gh api "repos/${repo}" --jq .default_branch)"

  echo "== ${repo} (reviewed ${reviewed:0:7}, since ${since})"

  echo "-- commits since the reviewed commit (${default_branch})"
  gh api "repos/${repo}/compare/${reviewed}...${default_branch}" \
    --jq '.commits[] | "\(.sha[0:7]) \(.commit.committer.date[0:10]) \(.commit.message | split("\n")[0])"'

  echo "-- issues and pull requests updated since ${since}"
  gh api --paginate "repos/${repo}/issues?state=all&sort=updated&direction=desc&since=${since}T00:00:00Z&per_page=100" \
    --jq '.[] | "\(if .pull_request then "PR" else "issue" end) #\(.number) [\(.state)] \(.updated_at[0:10]) \(.title)"'

  echo
done
