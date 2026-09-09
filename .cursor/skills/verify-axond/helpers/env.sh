#!/usr/bin/env bash
# Source after launch:  source .cursor/skills/verify-axond/helpers/env.sh
# Requires AXOND_VERIFY_RUN.

_verify_axond_env_helpers="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${_verify_axond_env_helpers}/lib.sh"
verify_axond_load_run "$(verify_axond_run_id)"
