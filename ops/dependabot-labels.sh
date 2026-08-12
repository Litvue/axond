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

# The labels are the `- value` entries indented under a `labels:` key. Anything
# that starts at or left of the key itself ends the block, list items included:
# the next `- package-ecosystem:` entry is not a label.
read_labels() {
    awk '
        /^[[:space:]]*labels:[[:space:]]*$/ { indent = match($0, /[^ ]/); inside = 1; next }
        inside && !/^[[:space:]]*(#|$)/ && match($0, /[^ ]/) <= indent { inside = 0 }
        inside && /^[[:space:]]*-[[:space:]]*/ {
            line = $0
            sub(/^[[:space:]]*-[[:space:]]*/, "", line)
            gsub(/^["'\''"]|["'\''"]$/, "", line)
            print line
        }
    ' "$1" | sort -u
}

# Reading nothing must not look like "no labels to check": a moved, renamed, or
# reformatted config would then pass this gate with the labels unverified, which
# is the silent failure the script exists to prevent.
collect_labels() {
    local config=$1 found
    if [[ ! -f $config ]]; then
        echo "$config is missing; nothing would refresh the Action pins" >&2
        return 1
    fi
    found="$(read_labels "$config")"
    if [[ -z $found ]] && grep -Eq '^[[:space:]]*labels:' "$config"; then
        echo "$config declares labels this parser cannot read (an inline list?)" >&2
        echo "Write them as a block list, or teach ops/dependabot-labels.sh the shape." >&2
        return 1
    fi
    [[ -n $found ]] && printf '%s\n' "$found"
    return 0
}

# Each case is a way this check could pass while verifying nothing: a neighbouring
# key read as a label, a config that moved, or a shape the parser cannot see.
self_test() {
    local work status problems=0
    work="$(mktemp -d)"
    trap 'rm -rf "$work"' RETURN

    cat >"$work/full.yml" <<'YAML'
version: 2
updates:
  - package-ecosystem: github-actions
    directory: /
    labels:
      - area:operations
      # a comment inside the block
      - "quoted:label"
  - package-ecosystem: docker
    directory: /
    labels:
      - area:operations
    commit-message:
      prefix: ci
YAML
    printf 'version: 2\nupdates:\n  - package-ecosystem: github-actions\n    labels: [area:operations]\n' >"$work/inline.yml"
    printf 'version: 2\nupdates:\n  - package-ecosystem: github-actions\n    directory: /\n' >"$work/none.yml"

    local found
    found="$(collect_labels "$work/full.yml" | tr '\n' ' ')"
    if [[ ${found% } != 'area:operations quoted:label' ]]; then
        echo "self-test: read '${found% }', expected 'area:operations quoted:label'" >&2
        problems=1
    fi

    for unreadable in "$work/inline.yml" "$work/missing.yml"; do
        status=0
        collect_labels "$unreadable" >/dev/null 2>&1 || status=$?
        if ((status == 0)); then
            echo "self-test: $(basename "$unreadable") was accepted, expected a failure" >&2
            problems=1
        fi
    done

    status=0
    found="$(collect_labels "$work/none.yml")" || status=$?
    if ((status != 0)) || [[ -n $found ]]; then
        echo "self-test: a config with no labels should read as empty and succeed" >&2
        problems=1
    fi

    if ((problems)); then
        return 1
    fi
    echo "dependabot label parser self-test passed"
}

if [[ ${1:-} == --self-test ]]; then
    self_test
    exit
fi

# Not a process substitution: that would discard the exit status and turn an
# unreadable config back into a silent pass.
if ! labels_text="$(collect_labels "$CONFIG")"; then
    exit 1
fi

labels=()
if [[ -n $labels_text ]]; then
    mapfile -t labels <<<"$labels_text"
fi

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
