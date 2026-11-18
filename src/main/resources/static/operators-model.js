// Kubernetes logs can include terminal colour codes. Keep the viewer plain text.
export function visibleLogText(text, filter = '') {
  const clean = text.replace(/\x1b\[[0-?]*[ -/]*[@-~]/g, '')
    .replace(/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/g, '');
  const needle = filter.toLowerCase();
  return needle ? clean.split('\n').filter(line => line.toLowerCase().includes(needle)).join('\n') : clean;
}

const logEncoder = new TextEncoder();
const logDecoder = new TextDecoder();
export function appendLogText(previous, chunk, maxBytes = 262144) {
  const bytes = logEncoder.encode(previous + chunk);
  let start = Math.max(0, bytes.length - maxBytes);
  // Start at a UTF-8 code point boundary, without manufacturing replacement characters.
  while (start < bytes.length && (bytes[start] & 0xc0) === 0x80) start++;
  return logDecoder.decode(bytes.subarray(start));
}

export function reconnectBackoff() {
  let delay = 2000;
  return {
    next() { const current = delay; delay = Math.min(delay * 2, 30000); return current; },
    reset() { delay = 2000; }
  };
}

// A REST response/error begun before a pushed snapshot cannot undo that newer observation.
export function snapshotOrder() {
  let revision = 0;
  let observed = -Infinity;
  return {
    beginRequest() { return revision; },
    canReportError(request) { return request === revision; },
    accept(value, request = null) {
      if (request !== null && request !== revision) return false;
      const at = Date.parse(value.observedAt);
      if (Number.isFinite(at) && at < observed) return false;
      if (Number.isFinite(at)) observed = at;
      else if (!value.enabled) observed = -Infinity;
      revision++; return true;
    }
  };
}
