default:
    @just --list

test:
    cd ts && npm test

check:
    cd ts && npm test && npm run lint:web && npm run check:alerts && npm run check:docs && npm run check:npm

run:
    ts/bin/axond --config axond.toml

workflow-policy:
    ops/workflow-policy.py --self-test
    ops/workflow-policy.py

actionlint:
    ops/actionlint.sh --self-test
    ops/actionlint.sh

quickstart-smoke:
    ops/compose-smoke.sh
