import { visibleLogText, appendLogText, reconnectBackoff, snapshotOrder } from './operators-model.js';
// All cluster-supplied values, including log lines, are rendered as text.
const byId = id => document.getElementById(id);
const node = (tag, className, text) => {
  const result = document.createElement(tag);
  if (className) result.className = className;
  if (text != null) result.textContent = text;
  return result;
};
const tone = state => ['STABLE', 'READY', 'RUNNING', 'DEPLOYED'].includes(state) ? 'healthy' :
  ['FAILED', 'ERROR'].includes(state) ? 'failed' : ['UPGRADING', 'ROLLING_BACK', 'DEPLOYING', 'CREATED'].includes(state) ? 'pending' : 'neutral';
const badge = state => node('span', `state ${tone(state)}`, state || 'Unknown');
const time = at => at && !Number.isNaN(new Date(at).getTime()) ? new Date(at).toLocaleString() : '—';

export function initializeOperators() {
  let active = false;
  let loading = false;
  let snapshot = null;
  let targets = [];
  let logText = '';
  let logRequest = null;
  let logSequence = 0;
  let logsLoading = false;
  let statusStream = null;
  let liveWatching = false;
  let logStream = null;
  let followWanted = false;
  let statusRetry = null;
  let logRetry = null;
  const statusBackoff = reconnectBackoff();
  const logBackoff = reconnectBackoff();
  const order = snapshotOrder();
  const tabs = [byId('clusters-tab'), byId('operators-tab')];
  const panels = [byId('clusters-view'), byId('operators-view')];
  function selectTab(index) {
    active = index === 1;
    tabs.forEach((tab, i) => { tab.setAttribute('aria-selected', String(i === index)); tab.tabIndex = i === index ? 0 : -1; panels[i].hidden = i !== index; });
    if (active) { refresh(); }
    else { const wasLive = followWanted; closeStatus(); cancelLogs(); followWanted = false; updateLogButtons(); if (wasLive) byId('operator-log-status').textContent = 'Paused while the Operators tab is closed. Start live logs to resume.'; }
  }
  tabs.forEach((tab, index) => {
    tab.addEventListener('click', () => selectTab(index));
    tab.addEventListener('keydown', event => {
      if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
      event.preventDefault();
      const next = event.key === 'Home' ? 0 : event.key === 'End' ? 1 : 1 - index;
      selectTab(next); tabs[next].focus();
    });
  });
  function cancelLogs() {
    clearTimeout(logRetry); logRetry = null;
    logStream?.close(); logStream = null;
    logSequence++;
    logRequest?.abort(); logRequest = null; logsLoading = false;
    byId('operator-load-logs').disabled = !byId('operator-log-target').value;
  }
  function resetLogs() {
    followWanted = false; cancelLogs(); updateLogButtons(); logText = '';
    byId('operator-log-output').textContent = 'No logs loaded.';
    byId('operator-log-status').textContent = 'Recent logs · rolling 256 KiB buffer. Load a snapshot or start live logs.';
  }
  function renderLogs() {
    const filter = byId('operator-log-filter').value.toLowerCase();
    const output = byId('operator-log-output');
    output.textContent = visibleLogText(logText, filter) || (filter ? 'No matching log lines.' : 'No log lines returned.');
    if (byId('operator-log-autoscroll').checked) output.scrollTop = output.scrollHeight;
  }
  function renderResources() {
    const namespace = byId('operator-resource-namespace').value;
    const resources = snapshot.resources.filter(resource => !namespace || resource.namespace === namespace);
    byId('operator-resource-count').textContent = resources.length;
    byId('operator-resources').replaceChildren(...resources.map(resource => {
      const row = node('tr');
      const identity = node('td'); identity.append(node('strong', '', resource.name), node('small', '', `${resource.namespace} · ${resource.mode === 'APPLICATION' ? 'Application' : 'Session'}`));
      row.append(identity);
      for (const [index, state] of [resource.lifecycleState, resource.reconciliationState, resource.jobManagerStatus, resource.jobState].entries()) {
        const cell = node('td'); cell.append(index === 3 && resource.mode === 'SESSION' ? node('span', 'state neutral', 'Not tracked') : badge(state)); row.append(cell);
      }
      const detail = node('td', 'resource-message', resource.pendingSpec === true ? 'Pending spec change' : resource.pendingSpec === false ? 'Spec observed' : 'Spec observation unknown');
      if (resource.error) detail.append(node('p', 'monitor-failure', resource.error));
      row.append(detail); return row;
    }));
    if (!resources.length) { const row = node('tr'); const cell = node('td', 'monitor-note', 'No resources found.'); cell.colSpan = 6; row.append(cell); byId('operator-resources').append(row); }
    const events = snapshot.events.filter(event => !namespace || event.namespace === namespace);
    byId('operator-events').replaceChildren(...events.map(event => {
      const item = node('li', event.type === 'Warning' ? 'warning' : '');
      item.append(node('span', 'event-time', time(event.at)), node('strong', '', `${event.namespace}/${event.resource} · ${event.reason}`), node('span', '', event.message), node('small', '', `${event.type} · ${event.count} occurrence${event.count === 1 ? '' : 's'}`));
      return item;
    }));
    if (!events.length) byId('operator-events').append(node('li', 'monitor-note', 'No retained events.'));
  }
  function render() {
    byId('operators-disabled').hidden = snapshot.enabled;
    byId('operators-content').hidden = !snapshot.enabled;
    if (!snapshot.enabled) { targets = []; byId('operator-log-target').replaceChildren(new Option('Select a container', '')); resetLogs(); return; }
    byId('operators-cards').replaceChildren(...snapshot.operators.map(operator => {
      const card = node('article', 'cluster-card operator-card');
      const header = node('div', 'card-heading'); const identity = node('div', 'cluster-identity');
      identity.append(node('span', 'namespace', operator.namespace), node('h3', 'cluster-name', operator.name));
      header.append(identity, badge(operator.readyReplicas >= operator.replicas && operator.replicas > 0 ? 'READY' : 'NOT READY'));
      card.append(header, node('div', 'cluster-meta', `Version ${operator.version || 'unknown'} · ${operator.readyReplicas}/${operator.replicas} replicas ready`));
      for (const image of operator.images) card.append(node('div', 'cluster-image', image));
      const pods = node('ul', 'operator-pods');
      for (const pod of operator.pods) {
        const item = node('li'); item.append(node('strong', '', pod.name), node('span', '', `${pod.ready ? 'Ready' : pod.phase} · ${pod.restarts} restarts`)); pods.append(item);
      }
      if (!operator.pods.length) pods.append(node('li', '', 'No owned Pods found.'));
      card.append(pods); return card;
    }));
    byId('operators-empty').hidden = snapshot.operators.length > 0;
    const nsSelect = byId('operator-resource-namespace'); const nsValue = nsSelect.value;
    const namespaceValues = [...new Set(snapshot.resources.map(resource => resource.namespace))].sort();
    if ([...nsSelect.options].map(option => option.value).join(',') !== ['', ...namespaceValues].join(',')) {
      nsSelect.replaceChildren(new Option('All namespaces', ''), ...namespaceValues.map(ns => new Option(ns, ns)));
      if (namespaceValues.includes(nsValue)) nsSelect.value = nsValue;
    }
    renderResources();
    const targetSelect = byId('operator-log-target'); const previous = targetSelect.value;
    targets = snapshot.operators.flatMap(operator => operator.pods.flatMap(pod => pod.containers.map(container => ({
      namespace: operator.namespace, operator: operator.name, pod: pod.name, container,
      id: JSON.stringify([operator.namespace, operator.name, pod.name, pod.uid, container])
    }))));
    if ([...targetSelect.options].slice(1).map(option => option.value).join('|') !== targets.map(target => target.id).join('|')) {
      targetSelect.replaceChildren(new Option('Select a container', ''), ...targets.map(target => new Option(`${target.namespace}/${target.pod} · ${target.container}`, target.id)));
      if (targets.some(target => target.id === previous)) targetSelect.value = previous;
      else resetLogs();
    }
    byId('operator-load-logs').disabled = logsLoading || !targetSelect.value;
    updateLogButtons();
  }
  function applySnapshot(value, request = null) {
    if (!order.accept(value, request)) return;
    snapshot = value;
    byId('operators-errors').hidden = !snapshot.errors.length;
    byId('operators-errors').textContent = snapshot.errors.map(error => `${error.namespace} · ${error.section}: ${error.reason === 'forbidden' ? 'Access forbidden. Check Portal RBAC.' : 'Unavailable. Retrying shortly.'}`).join('\n');
    byId('operators-updated').textContent = snapshot.observedAt ? `${liveWatching ? 'Live' : 'Observed'} · ${time(snapshot.observedAt)}` : 'Monitoring disabled';
    render();
  }
  function closeStatus() {
    clearTimeout(statusRetry); statusRetry = null;
    statusStream?.close(); statusStream = null; liveWatching = false;
  }
  function openStatus() {
    if (statusStream || !active || document.hidden) return;
    clearTimeout(statusRetry); statusRetry = null;
    const stream = new EventSource('/api/operators/stream');
    statusStream = stream;
    stream.onopen = () => { if (statusStream === stream) statusBackoff.reset(); };
    stream.addEventListener('snapshot', event => {
      if (statusStream !== stream) return;
      try {
        const update = JSON.parse(event.data);
        liveWatching = update.watching;
        applySnapshot(update.snapshot);
        if (!liveWatching) byId('operators-updated').textContent = `Watches reconnecting · snapshot ${time(snapshot.observedAt)}`;
      } catch { stream.close(); statusStream = null; liveWatching = false; }
    });
    stream.onerror = () => {
      if (statusStream !== stream) return;
      stream.close(); statusStream = null; liveWatching = false;
      if (active && !document.hidden) statusRetry = setTimeout(openStatus, statusBackoff.next());
      byId('operators-updated').textContent = `Updates reconnecting · last observed ${time(snapshot?.observedAt)}`;
    };
  }
  function updateLogButtons() {
    const selected = !!byId('operator-log-target').value;
    byId('operator-live-logs').disabled = !selected;
    byId('operator-live-logs').textContent = followWanted ? 'Pause live logs' : 'Start live logs';
    byId('operator-live-logs').setAttribute('aria-pressed', String(followWanted));
  }
  function startLiveLogs() {
    const target = targets.find(value => value.id === byId('operator-log-target').value);
    if (!target || !active || document.hidden) return;
    cancelLogs(); followWanted = true; updateLogButtons();
    const query = new URLSearchParams({ pod: target.pod, container: target.container, tailLines: byId('operator-log-lines').value });
    const stream = new EventSource(`/api/operators/${encodeURIComponent(target.namespace)}/${encodeURIComponent(target.operator)}/logs/stream?${query}`);
    logStream = stream;
    byId('operator-log-status').textContent = 'Connecting live logs…';
    stream.addEventListener('start', () => {
      if (logStream !== stream) return;
      logBackoff.reset(); logText = ''; renderLogs();
      byId('operator-log-status').textContent = 'Live logs · recent tail reloaded · rolling 256 KiB buffer';
    });
    stream.addEventListener('log', event => {
      if (logStream !== stream) return;
      try { logText = appendLogText(logText, JSON.parse(event.data).text); renderLogs(); }
      catch { cancelLogs(); followWanted = false; updateLogButtons(); byId('operator-log-status').textContent = 'Could not read log stream. Start live logs to retry.'; }
    });
    stream.addEventListener('reconnect', event => {
      if (logStream === stream) byId('operator-log-status').textContent = event.data;
    });
    stream.onerror = () => {
      if (logStream !== stream) return;
      stream.close(); logStream = null;
      if (followWanted && active && !document.hidden) logRetry = setTimeout(startLiveLogs, logBackoff.next());
      byId('operator-log-status').textContent = 'Log stream reconnecting · recent tail will reload. If access is denied, check Portal RBAC.';
    };
  }
  async function refresh() {
    if (!active || loading) return;
    const request = order.beginRequest();
    loading = true; byId('operators-refresh').disabled = true;
    try {
      const response = await fetch('/api/operators', { cache: 'no-store', signal: AbortSignal.timeout(20000) });
      if (!response.ok) throw new Error();
      applySnapshot(await response.json(), request);
      if (snapshot.enabled && active && !document.hidden) openStatus();
    } catch {
      if (!order.canReportError(request) || liveWatching) return;
      byId('operators-errors').hidden = false;
      byId('operators-errors').textContent = 'Operator discovery unavailable. Retrying shortly.';
      byId('operators-updated').textContent = 'Connection lost';
      // Do not present old health or log targets as current after an unsuccessful refresh.
      byId('operators-content').hidden = true; resetLogs();
    } finally { loading = false; byId('operators-refresh').disabled = false; }
  }
  async function loadLogs() {
    const target = targets.find(value => value.id === byId('operator-log-target').value);
    if (!target) return;
    followWanted = false; cancelLogs(); updateLogButtons(); const sequence = logSequence;
    logsLoading = true; byId('operator-load-logs').disabled = true;
    logRequest = new AbortController();
    byId('operator-log-status').textContent = 'Loading recent logs…';
    const query = new URLSearchParams({ pod: target.pod, container: target.container, tailLines: byId('operator-log-lines').value });
    try {
      const response = await fetch(`/api/operators/${encodeURIComponent(target.namespace)}/${encodeURIComponent(target.operator)}/logs?${query}`, {
        cache: 'no-store', signal: AbortSignal.any([logRequest.signal, AbortSignal.timeout(15000)])
      });
      if (!response.ok) throw new Error(response.status === 403 ? 'Log access forbidden. Check Portal RBAC.' : response.status === 404 ? 'Operator or container no longer available.' : 'Logs unavailable. Retry to refresh.');
      const result = await response.json();
      if (sequence !== logSequence) return;
      logText = result.text; renderLogs();
      byId('operator-log-status').textContent = `Loaded ${new Date().toLocaleTimeString()} · last hour · up to ${byId('operator-log-lines').value} lines / 256 KiB · manual refresh`;
    } catch (error) {
      if (sequence !== logSequence) return;
      logText = ''; byId('operator-log-output').textContent = 'No logs loaded.';
      byId('operator-log-status').textContent = error.name === 'TimeoutError' ? 'Log request timed out. Retry to refresh.' : error.message;
    } finally {
      if (sequence === logSequence) { logsLoading = false; logRequest = null; byId('operator-load-logs').disabled = !byId('operator-log-target').value; }
    }
  }
  byId('operators-refresh').addEventListener('click', refresh);
  byId('operator-resource-namespace').addEventListener('change', renderResources);
  for (const id of ['operator-log-target', 'operator-log-lines']) byId(id).addEventListener('change', resetLogs);
  byId('operator-log-filter').addEventListener('input', () => { if (logText) renderLogs(); });
  byId('operator-load-logs').addEventListener('click', loadLogs);
  byId('operator-live-logs').addEventListener('click', () => {
    if (followWanted) { followWanted = false; cancelLogs(); updateLogButtons(); byId('operator-log-status').textContent = 'Paused · resume reloads the recent tail.'; }
    else startLiveLogs();
  });
  byId('operator-log-autoscroll').addEventListener('change', renderLogs);
  document.addEventListener('visibilitychange', () => {
    if (document.hidden) { closeStatus(); cancelLogs(); if (followWanted) byId('operator-log-status').textContent = 'Live logs paused while this page is hidden.'; }
    else if (active) { refresh(); if (followWanted) startLiveLogs(); }
  });
  window.addEventListener('pagehide', () => { closeStatus(); cancelLogs(); });
  setInterval(() => { if (active && !document.hidden && !liveWatching) refresh(); }, 10000);
}
