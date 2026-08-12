#!/usr/bin/env bash
# Dependabot rejects its *entire* configuration when a label it applies does not
# exist, and reports that only on the repository's Dependabot page. Since the
# Action pins are refreshed by nothing but that weekly pull request, a renamed
# label would stop the refreshes with no failing check anywhere. This turns that
# silent failure into a loud one.
#
# It needs the GitHub API, so it is skipped rather than failed when no token is
# available: the offline gate is ops/workflow-policy.py.
set -euo pipefail

cd "$(dirname "$0")/.."
CONFIG=.github/dependabot.yml
REPOSITORY="${GITHUB_REPOSITORY:-Litvue/axond}"

# The labels are the `- value` entries under a `labels:` key, up to the next key
# at the same or lower indentation.
mapfile -t labels < <(
    awk '
        /^[[:space:]]*labels:[[:space:]]*$/ { indent = match($0, /[^ ]/); inside = 1; next }
        inside && /^[[:space:]]*-[[:space:]]*/ {
            line = $0
            sub(/^[[:space:]]*-[[:space:]]*/, "", line)
            gsub(/^["'\''"]|["'\''"]$/, "", line)
            print line
            next
        }
        inside && /^[[:space:]]*[^[:space:]#-]/ { if (match($0, /[^ ]/) <= indent) inside = 0 }
    ' "$CONFIG" | sort -u
)

if ((${#labels[@]} == 0)); then
    echo "$CONFIG applies no labels; nothing to verify"
    exit 0
fi

if ! command -v gh >/dev/null 2>&1 || ! gh auth status >/dev/null 2>&1; then
    echo "skipping the label check: no authenticated gh for $REPOSITORY"
    echo "labels $CONFIG needs: ${labels[*]}"
    exit 0
fi

existing="$(gh api "repos/$REPOSITORY/labels" --paginate --jq '.[].name')"

missing=()
for label in "${labels[@]}"; do
    if ! grep -Fxq "$label" <<<"$existing"; then
        missing+=("$label")
    fi
done

if ((${#missing[@]})); then
    printf '%s applies labels that do not exist on %s: %s\n' \
        "$CONFIG" "$REPOSITORY" "${missing[*]}" >&2
    echo "Dependabot rejects the whole configuration on an unknown label, so it" >&2
    echo "would stop proposing Action pin bumps. Create the label or fix the name." >&2
    exit 1
fi

echo "dependabot labels exist on $REPOSITORY (${labels[*]})"
