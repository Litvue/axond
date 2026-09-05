#!/usr/bin/env python3
"""Classify the complete event diff; unknown paths keep Axond CI enabled."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile


def git(*args):
    return subprocess.check_output(['git', *args], stderr=subprocess.PIPE)


def detect(event, payload, ref):
    # Tags and explicit qualification requests always exercise the full suite.
    if event == 'workflow_dispatch' or ref.startswith('refs/tags/'):
        return {'rust': 'true', 'dependencies': 'true'}
    if event == 'pull_request':
        base = payload['pull_request']['base']['sha']
        head = payload['pull_request']['head']['sha']
        base = git('merge-base', base, head).decode().strip()
    elif event == 'merge_group':
        base = payload['merge_group']['base_sha']
        head = payload['merge_group']['head_sha']
    elif event == 'push':
        base, head = payload['before'], payload['after']
        if base == '0' * 40:
            return {'rust': 'true', 'dependencies': 'true'}
    else:
        raise ValueError(f'unsupported CI event: {event}')
    # --no-renames includes both sides of a move across the website boundary.
    paths = git('diff', '--name-only', '--no-renames', '-z', base, head).decode().split('\0')
    paths = [path for path in paths if path]
    rust = any(not (path.startswith('website/') or path == '.github/workflows/website.yml') for path in paths)
    dependency_paths = {'Cargo.lock', 'Cargo.toml', 'fuzz/Cargo.lock', 'fuzz/Cargo.toml',
                        'deny.toml', '.github/workflows/ci.yml', 'ops/ci-changes.py'}
    dependencies = rust and (event != 'pull_request' or any(
        path in dependency_paths or (path.startswith('crates/') and path.endswith('/Cargo.toml'))
        for path in paths))
    return {'rust': str(rust).lower(), 'dependencies': str(dependencies).lower()}


def self_test():
    original = Path.cwd()
    try:
        with tempfile.TemporaryDirectory() as directory:
            os.chdir(directory)
            git('init', '--quiet'); git('config', 'user.email', 'ci@example.invalid')
            git('config', 'user.name', 'CI scope tests')
            git('commit', '--allow-empty', '-qm', 'base')
            base = git('rev-parse', 'HEAD').decode().strip()
            def commit(path):
                p = Path(path); p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text('changed\n'); git('add', '.'); git('commit', '-qm', 'change')
                return git('rev-parse', 'HEAD').decode().strip()
            website = commit('website/src/pages/index.astro')
            workflow = commit('.github/workflows/website.yml')
            source = commit('crates/gateway/src/main.rs')
            lock = commit('Cargo.lock')
            for head, rust, dependencies in [(website, 'false', 'false'), (workflow, 'false', 'false'),
                                              (source, 'true', 'false'), (lock, 'true', 'true')]:
                payload = {'pull_request': {'base': {'sha': base}, 'head': {'sha': head}}}
                assert detect('pull_request', payload, '') == {'rust': rust, 'dependencies': dependencies}
                for event, payload in [('push', {'before': base, 'after': head}),
                                       ('merge_group', {'merge_group': {'base_sha': base, 'head_sha': head}})]:
                    assert detect(event, payload, '') == {'rust': rust, 'dependencies': rust}
            # A last-commit-only diff would miss the earlier source change.
            last = commit('website/README.md')
            assert detect('push', {'before': workflow, 'after': last}, '')['rust'] == 'true'
            # PRs compare with the merge base, not unrelated updates on main.
            git('checkout', '-qb', 'other-main', base)
            other = commit('Cargo.toml')
            assert detect('pull_request', {'pull_request': {'base': {'sha': other}, 'head': {'sha': website}}}, '')['rust'] == 'false'
            git('checkout', '-q', last)
            git('mv', 'crates/gateway/src/main.rs', 'website/moved.rs'); git('commit', '-qm', 'move')
            moved = git('rev-parse', 'HEAD').decode().strip()
            assert detect('push', {'before': last, 'after': moved}, '')['rust'] == 'true'
            git('rm', 'website/moved.rs'); git('commit', '-qm', 'delete website file')
            deleted = git('rev-parse', 'HEAD').decode().strip()
            assert detect('push', {'before': moved, 'after': deleted}, '')['rust'] == 'false'
            assert detect('push', {}, 'refs/tags/v1.0.0')['rust'] == 'true'
            assert detect('workflow_dispatch', {}, '')['rust'] == 'true'
            assert detect('push', {'before': '0'*40, 'after': deleted}, '')['rust'] == 'true'
            for path, dependencies in [('install.sh', 'false'), ('future-component/source.rs', 'false'),
                                       ('crates/gateway/Cargo.toml', 'true'), ('fuzz/Cargo.lock', 'true')]:
                previous = git('rev-parse', 'HEAD').decode().strip()
                head = commit(path)
                payload = {'pull_request': {'base': {'sha': previous}, 'head': {'sha': head}}}
                assert detect('pull_request', payload, '') == {'rust': 'true', 'dependencies': dependencies}
            try:
                detect('push', {'before': 'missing-ref', 'after': deleted}, '')
            except subprocess.CalledProcessError:
                pass
            else:
                raise AssertionError('invalid diff must fail closed')
    finally:
        os.chdir(original)
    print('CI change detection passed: PR, multi-commit push, merge queue, rename, deletion, tag, manual, invalid diff')


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-test']:
        self_test()
    else:
        result = detect(os.environ['GITHUB_EVENT_NAME'],
                        json.loads(Path(os.environ['GITHUB_EVENT_PATH']).read_text()),
                        os.environ['GITHUB_REF'])
        with open(os.environ['GITHUB_OUTPUT'], 'a') as output:
            for key, value in result.items():
                print(f'{key}={value}', file=output)
                print(f'{key}={value}')
