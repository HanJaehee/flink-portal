import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
const source = await readFile(new URL('../main/resources/static/portal-model.js', import.meta.url), 'utf8');
const { clusterStates, matchesCluster, clusterLinks } = await import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`);
const id = '0123456789abcdef0123456789abcdef';
const cluster = { name: 'stream-a', namespace: 'analytics', mode: 'APPLICATION', applicationJobId: id,
  uiPath: '/clusters/analytics/stream-a/', uiAvailable: true,
  jobs: [{ id, name: 'Orders', state: 'RUNNING' }, { id: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', name: 'Previous', state: 'FAILED' }] };
test('mixed session states match independently and compose with namespace and search', () => {
  assert.deepEqual(clusterStates(cluster), ['RUNNING', 'FAILED']);
  assert.equal(matchesCluster(cluster, 'orders', 'analytics', 'FAILED'), true);
  assert.equal(matchesCluster(cluster, '', 'other', 'RUNNING'), false);
  assert.equal(matchesCluster(cluster, 'absent', '', 'RUNNING'), false);
  assert.equal(matchesCluster(cluster, '', '', 'FINISHED'), false);
});
test('unavailable status never falls back to stale operator job state', () => {
  assert.deepEqual(clusterStates({ ...cluster, jobs: null, jobState: 'RUNNING' }), ['UNKNOWN']);
  assert.deepEqual(clusterStates({ ...cluster, jobs: [], reason: 'suspended' }), ['SUSPENDED']);
  assert.deepEqual(clusterStates({ ...cluster, jobs: [] }), ['NO_JOBS']);
});
test('all shortcuts stay under the portal proxy and application details target the current job', () => {
  assert.deepEqual(clusterLinks(cluster), {
    ui: '/clusters/analytics/stream-a/', metrics: '/clusters/analytics/stream-a/#/job-manager/metrics',
    logs: '/clusters/analytics/stream-a/#/job-manager/log',
    job: '/clusters/analytics/stream-a/#/job/running/0123456789abcdef0123456789abcdef/overview'
  });
  assert.equal(clusterLinks({ ...cluster, jobs: [{ id, state: 'FINISHED' }] }).job,
    '/clusters/analytics/stream-a/#/job/completed/0123456789abcdef0123456789abcdef/overview');
  for (const change of [{ mode: 'SESSION' }, { applicationJobId: null }, { applicationJobId: 'not-a-job' }, { jobs: [] }]) {
    assert.equal(clusterLinks({ ...cluster, ...change }).job, null);
  }
});
test('unavailable clusters and unsafe paths have no active shortcuts', () => {
  for (const change of [{ uiAvailable: false }, { uiPath: '//evil.test/' }, { uiPath: '/clusters/a/../' }, { uiPath: '/clusters/a/../../evil/' }, { uiPath: '/clusters/a/b/\\evil' }]) {
    assert.deepEqual(clusterLinks({ ...cluster, ...change }), { ui: null, metrics: null, logs: null, job: null });
  }
});
