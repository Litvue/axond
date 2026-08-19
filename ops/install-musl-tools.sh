#!/usr/bin/env bash
# Install the native compiler required by Rust's Linux musl targets without
# allowing a wedged package mirror or dpkg process to consume the runner's
# six-hour default job limit. Acquire retries cover transient mirror failures;
# the outer loop also retries timeouts and package-manager failures.
set -euo pipefail

readonly APT_ATTEMPTS=3
readonly APT_TIMEOUT_SECONDS=180
readonly APT_KILL_AFTER_SECONDS=15
readonly RETRY_DELAY_SECONDS=5

musl_is_available() {
    command -v musl-gcc >/dev/null 2>&1
}

run_bounded_root() {
    # Run timeout as root with the package tool so it can terminate the complete
    # command on expiry. `sudo -n` makes a credential prompt fail immediately.
    sudo -n env DEBIAN_FRONTEND=noninteractive \
        timeout --signal=TERM --kill-after="${APT_KILL_AFTER_SECONDS}s" \
        "${APT_TIMEOUT_SECONDS}s" "$@"
}

run_bounded_apt() {
    run_bounded_root apt-get -o Acquire::Retries=3 -o Dpkg::Use-Pty=0 "$@"
}

repair_dpkg() {
    # A timed-out install may leave unpacked packages awaiting configuration.
    # Repair that state before retrying apt; this is bounded by the same limit.
    run_bounded_root dpkg --configure -a
}

install_once() {
    run_bounded_apt update
    run_bounded_apt install -y --no-install-recommends musl-tools
}

retry_sleep() {
    sleep "$1"
}

install_musl_tools() {
    if musl_is_available; then
        echo "musl-gcc is already installed"
        return 0
    fi

    local attempt delay
    for ((attempt = 1; attempt <= APT_ATTEMPTS; attempt++)); do
        echo "installing musl-tools (attempt ${attempt}/${APT_ATTEMPTS})"
        if ((attempt > 1)); then
            echo "repairing any interrupted dpkg transaction before retry"
            if ! repair_dpkg; then
                echo "dpkg repair failed on attempt ${attempt}/${APT_ATTEMPTS}" >&2
            elif install_once && musl_is_available; then
                echo "musl-tools installed successfully"
                return 0
            fi
        elif install_once && musl_is_available; then
            echo "musl-tools installed successfully"
            return 0
        fi

        if ((attempt < APT_ATTEMPTS)); then
            delay=$((attempt * RETRY_DELAY_SECONDS))
            echo "musl-tools installation failed; retrying in ${delay}s" >&2
            retry_sleep "$delay"
        fi
    done

    echo "musl-tools installation failed after ${APT_ATTEMPTS} bounded attempts" >&2
    return 1
}

self_test() {
    local actual expected attempts repairs sleeps status problems=0

    # Assert that both apt phases are wrapped in the reviewed timeout and apt
    # retry options; replacing this with a stubbed success would make the retry
    # loop's tests pass while losing the bound that this script exists to add.
    sudo() {
        printf '%s\n' "$*"
    }
    actual="$(run_bounded_apt update)"
    expected="-n env DEBIAN_FRONTEND=noninteractive timeout --signal=TERM --kill-after=15s 180s apt-get -o Acquire::Retries=3 -o Dpkg::Use-Pty=0 update"
    if [[ $actual != "$expected" ]]; then
        echo "self-test: bounded apt command was '$actual', expected '$expected'" >&2
        problems=1
    fi
    # The test double below deliberately replaces this production definition
    # only after its exact command has been asserted here.
    # shellcheck disable=SC2218
    actual="$(repair_dpkg)"
    expected="-n env DEBIAN_FRONTEND=noninteractive timeout --signal=TERM --kill-after=15s 180s dpkg --configure -a"
    if [[ $actual != "$expected" ]]; then
        echo "self-test: bounded dpkg repair was '$actual', expected '$expected'" >&2
        problems=1
    fi

    # A transient first failure must retry once, apply the documented backoff,
    # and require musl-gcc to exist before reporting success.
    attempts=0
    repairs=0
    sleeps=
    musl_is_available() {
        [[ $attempts -ge 2 ]]
    }
    install_once() {
        attempts=$((attempts + 1))
        [[ $attempts -ge 2 ]]
    }
    repair_dpkg() {
        repairs=$((repairs + 1))
    }
    retry_sleep() {
        sleeps="${sleeps}${sleeps:+ }$1"
    }
    status=0
    install_musl_tools >/dev/null 2>&1 || status=$?
    if ((status != 0)) || ((attempts != 2)) || ((repairs != 1)) || [[ $sleeps != 5 ]]; then
        echo "self-test: transient failure used status=$status attempts=$attempts repairs=$repairs sleeps='$sleeps'" >&2
        problems=1
    fi

    # A persistent failure must stop after the fixed attempt budget, sleeping
    # only between attempts (5s then 10s), never after the terminal failure.
    attempts=0
    repairs=0
    sleeps=
    musl_is_available() {
        return 1
    }
    install_once() {
        attempts=$((attempts + 1))
        return 1
    }
    repair_dpkg() {
        repairs=$((repairs + 1))
    }
    status=0
    install_musl_tools >/dev/null 2>&1 || status=$?
    if ((status == 0)) || ((attempts != APT_ATTEMPTS)) \
        || ((repairs != APT_ATTEMPTS - 1)) || [[ $sleeps != "5 10" ]]; then
        echo "self-test: persistent failure used status=$status attempts=$attempts repairs=$repairs sleeps='$sleeps'" >&2
        problems=1
    fi

    # An image that already has musl-gcc must not touch apt at all.
    attempts=0
    musl_is_available() {
        return 0
    }
    install_once() {
        attempts=$((attempts + 1))
    }
    status=0
    install_musl_tools >/dev/null 2>&1 || status=$?
    if ((status != 0)) || ((attempts != 0)); then
        echo "self-test: preinstalled musl used status=$status apt_calls=$attempts" >&2
        problems=1
    fi

    if ((problems)); then
        return 1
    fi
    echo "bounded musl installer self-test passed"
}

if [[ ${1-} == --self-test ]]; then
    self_test
    exit
fi
if (($#)); then
    echo "usage: $0 [--self-test]" >&2
    exit 2
fi

install_musl_tools
