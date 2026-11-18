import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
const source = await readFile(new URL('../main/resources/static/operators-model.js', import.meta.url), 'utf8');
const { visibleLogText } = await import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`);

test('operator logs remove terminal escapes while preserving plain text and line structure', () => {
  assert.equal(visibleLogText('\x1b[m\x1b[33mINFO\x1b[0m [team/stream] Ready\n<script>text</script>\t✓\x00'),
    'INFO [team/stream] Ready\n<script>text</script>\t✓');
});
test('operator log filter matches visible text without case sensitivity', () => {
  assert.equal(visibleLogText('\x1b[32mINFO\x1b[m [team/a] Ready\nERROR [team/b] Failed', 'TEAM/A'),
    'INFO [team/a] Ready');
  assert.equal(visibleLogText('INFO ready', 'failed'), '');
});

test('live log buffer retains only recent UTF8 bytes without splitting characters', async () => {
  const { appendLogText } = await import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`);
  assert.equal(appendLogText('old\n', '한한한', 7), '한한');
  assert.equal(appendLogText('abc', 'def', 4), 'cdef');
  assert.equal(appendLogText('', '🦊🦊', 5), '🦊');
});

test('reconnect backoff increases to a cap and resets after a recovered connection', async () => {
  const { reconnectBackoff } = await import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`);
  const backoff = reconnectBackoff();
  assert.deepEqual(Array.from({ length: 7 }, () => backoff.next()), [2000, 4000, 8000, 16000, 30000, 30000, 30000]);
  backoff.reset(); assert.equal(backoff.next(), 2000);
});
test('newer pushed state wins over delayed REST success and failure', async () => {
  const { snapshotOrder } = await import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`);
  const order = snapshotOrder();
  const oldRequest = order.beginRequest();
  assert.equal(order.accept({ observedAt: '2026-11-19T00:00:02Z' }), true);
  assert.equal(order.accept({ observedAt: '2026-11-19T00:00:01Z' }, oldRequest), false);
  assert.equal(order.canReportError(oldRequest), false);
  const current = order.beginRequest();
  assert.equal(order.canReportError(current), true);
  assert.equal(order.accept({ observedAt: '2026-11-19T00:00:01Z' }, current), false);
  assert.equal(order.accept({ observedAt: '2026-11-19T00:00:03Z' }, current), true);
});
