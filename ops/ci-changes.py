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
        return {'typescript': 'true'}
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
            return {'typescript': 'true'}
    else:
        raise ValueError(f'unsupported CI event: {event}')
    # --no-renames includes both sides of a move across the website boundary.
    paths = git('diff', '--name-only', '--no-renames', '-z', base, head).decode().split('\0')
    paths = [path for path in paths if path]
    typescript = any(not (path.startswith('website/') or path == '.github/workflows/website.yml') for path in paths)
    return {'typescript': str(typescript).lower()}


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
            image_workflow = commit('.github/workflows/typescript-image.yml')
            source = commit('ts/packages/gateway/src/app.ts')
            lock = commit('ts/package-lock.json')
            for head, selected in [(website, 'false'), (workflow, 'false'), (source, 'true'), (lock, 'true')]:
                payload = {'pull_request': {'base': {'sha': base}, 'head': {'sha': head}}}
                assert detect('pull_request', payload, '') == {'typescript': selected}
                for event, payload in [('push', {'before': base, 'after': head}),
                                       ('merge_group', {'merge_group': {'base_sha': base, 'head_sha': head}})]:
                    assert detect(event, payload, '') == {'typescript': selected}
            payload = {'pull_request': {'base': {'sha': workflow}, 'head': {'sha': image_workflow}}}
            assert detect('pull_request', payload, '') == {'typescript': 'true'}
            # A last-commit-only diff would miss the earlier source change.
            last = commit('website/README.md')
            assert detect('push', {'before': workflow, 'after': last}, '')['typescript'] == 'true'
            # PRs compare with the merge base, not unrelated updates on main.
            git('checkout', '-qb', 'other-main', base)
            other = commit('ts/package.json')
            assert detect('pull_request', {'pull_request': {'base': {'sha': other}, 'head': {'sha': website}}}, '')['typescript'] == 'false'
            git('checkout', '-q', last)
            git('mv', 'ts/packages/gateway/src/app.ts', 'website/moved.ts'); git('commit', '-qm', 'move')
            moved = git('rev-parse', 'HEAD').decode().strip()
            assert detect('push', {'before': last, 'after': moved}, '')['typescript'] == 'true'
            git('rm', 'website/moved.ts'); git('commit', '-qm', 'delete website file')
            deleted = git('rev-parse', 'HEAD').decode().strip()
            assert detect('push', {'before': moved, 'after': deleted}, '')['typescript'] == 'false'
            assert detect('push', {}, 'refs/tags/v1.0.0')['typescript'] == 'true'
            assert detect('workflow_dispatch', {}, '')['typescript'] == 'true'
            assert detect('push', {'before': '0'*40, 'after': deleted}, '')['typescript'] == 'true'
            for path in ['install.sh', 'future-component/source.ts', 'ts/package.json', 'ops/check-docs.py']:
                previous = git('rev-parse', 'HEAD').decode().strip()
                head = commit(path)
                payload = {'pull_request': {'base': {'sha': previous}, 'head': {'sha': head}}}
                assert detect('pull_request', payload, '') == {'typescript': 'true'}
            try:
                detect('push', {'before': 'missing-ref', 'after': deleted}, '')
            except subprocess.CalledProcessError:
                pass
            else:
                raise AssertionError('invalid diff must fail closed')
            previous = git('rev-parse', 'HEAD').decode().strip()
            typescript = commit('ts/packages/gateway/src/app.ts')
            assert detect('pull_request', {'pull_request': {'base': {'sha': previous}, 'head': {'sha': typescript}}}, '') == {'typescript': 'true'}
            mixed = commit('ops/future.py')
            assert detect('pull_request', {'pull_request': {'base': {'sha': previous}, 'head': {'sha': mixed}}}, '')['typescript'] == 'true'
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
