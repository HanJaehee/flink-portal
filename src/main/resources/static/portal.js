/* Resource names and status messages are always rendered as text, never markup. */
const byId = (id) => document.getElementById(id);
let clusters = [];
let loading = false;
const reasonText = {
  suspended: 'Suspended', 'missing-service': 'Starting', 'no-ready-endpoints': 'Waiting for endpoints',
  'rest-unreachable': 'Temporarily unavailable', 'ambiguous-service': 'Service conflict',
  'discovery-unavailable': 'Discovery unavailable'
};
function element(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text != null) node.textContent = text;
  return node;
}
function startedAt(value) {
  if (!value) return '—';
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? '—' : new Intl.DateTimeFormat(undefined, {
    year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit',
    second: '2-digit', hour12: false, timeZoneName: 'short'
  }).format(date);
}
function managerRow(label, manager, fresh) {
  const row = element('div', 'manager-resource');
  row.append(element('strong', '', `${label}: ${fresh && manager?.readyCount != null ? manager.readyCount : '—'} ready`));
  const allocation = value => `${value?.cpu ?? '—'} CPU · ${value?.memory ?? '—'} memory`;
  row.append(element('span', '', `Req: ${allocation(manager?.requests)}`));
  row.append(element('span', '', `Limit: ${allocation(manager?.limits)}`));
  return row;
}
function render() {
  const search = byId('search').value.toLowerCase().trim();
  const namespace = byId('namespace').value;
  const visible = clusters.filter(c => (!namespace || c.namespace === namespace) && `${c.name} ${c.namespace} ${(c.jobs || []).map(j => `${j.name} ${j.id} ${j.state}`).join(" ")}`.toLowerCase().includes(search));
  const rows = visible.map(c => {
    const row = element('tr');
    const nameCell = element('td');
    nameCell.append(element('span', 'cluster-name', c.name), element('span', 'version', `Apache Flink ${c.version}`));
    const ns = element('div', 'cluster-namespace'); ns.append(element('span', 'namespace', c.namespace));
    nameCell.append(ns, element('span', 'cluster-image', `Image: ${c.image || '—'}`));
    const stateCell = element('td', 'jobs-cell');
    if (Array.isArray(c.jobs) && c.jobs.length) {
      const list = element('ul', 'job-list');
      for (const job of c.jobs) {
        const item = element('li', 'job-item');
        const info = element('div', 'job-info');
        info.append(element('span', 'job-name', job.name || '(Unnamed job)'), element('span', 'job-id', job.id));
        const values = Array.isArray(job.parallelism) ? job.parallelism : [];
        const parallelism = values.length ? (values.length === 1 ? values[0] : `${values[0]}–${values[values.length - 1]}`) : '—';
        const parallelismNode = element('span', 'job-detail', `Parallelism: ${parallelism}`);
        parallelismNode.title = 'Parallelism across job vertices; a range means different vertices use different values.';
        info.append(parallelismNode, element('span', 'job-detail', `Started at: ${startedAt(job.startedAt)}`));
        const tone = ['FAILED', 'FAILING'].includes(job.state) ? 'failed' :
          ['RUNNING', 'FINISHED'].includes(job.state) ? 'healthy' :
          ['CREATED', 'INITIALIZING', 'RESTARTING', 'RECONCILING', 'CANCELLING'].includes(job.state) ? 'pending' : 'neutral';
        item.append(info, element('span', `state ${tone}`, job.state));
        list.append(item);
      }
      stateCell.append(list);
    } else {
      stateCell.append(element('span', 'job-placeholder', Array.isArray(c.jobs) ? 'No jobs' : 'Status unavailable'));
      if (!Array.isArray(c.jobs) && c.jobState && c.jobState !== 'UNKNOWN') {
        stateCell.append(element('span', 'version', `Operator: ${c.jobState}`));
      }
    }
    const resourceCell = element('td', 'resources-cell');
    resourceCell.append(managerRow('JM', c.jobManager, c.reason !== 'discovery-unavailable'), managerRow('TM', c.taskManager, c.reason !== 'discovery-unavailable'), element('span', 'resource-note', 'Per instance · configured requests / limits'));
    const uiCell = element('td', 'access-cell');
    const state = element('span', `availability ${c.uiAvailable ? '' : 'offline'}`);
    state.append(element('i', `dot ${c.uiAvailable ? 'green' : 'amber'}`), document.createTextNode(c.uiAvailable ? 'Available' : (reasonText[c.reason] || 'Unavailable')));
    uiCell.append(state);
    const actionCell = element('td');
    const link = element('a', `open ${c.uiAvailable ? '' : 'offline'}`, c.uiAvailable ? 'Open UI  ↗' : 'View status  ↗');
    // Paths come from the portal, but constrain them to this origin and cluster prefix.
    if (typeof c.uiPath === 'string' && c.uiPath.startsWith('/clusters/') && !c.uiPath.startsWith('//')) link.href = c.uiPath;
    link.target = '_blank'; link.rel = 'noopener noreferrer';
    link.setAttribute('aria-label', `${c.uiAvailable ? 'Open Flink UI' : 'View status'} for ${c.namespace}/${c.name}`);
    uiCell.append(link); row.append(nameCell, stateCell, resourceCell, uiCell); return row;
  });
  byId('clusters').replaceChildren(...rows);
  byId('total').textContent = clusters.length;
  byId('available').textContent = clusters.filter(c => c.uiAvailable).length;
  byId('unavailable').textContent = clusters.filter(c => !c.uiAvailable).length;
  byId('count').textContent = visible.length;
  byId('empty').hidden = visible.length !== 0;
  byId('empty-description').textContent = clusters.length ? 'Try another search or namespace.' : 'Clusters will appear as they are discovered.';
  byId('showing').textContent = `Showing ${visible.length} of ${clusters.length} clusters`;
}
async function refresh() {
  if (loading) return;
  loading = true; byId('refresh').disabled = true;
  try {
    const response = await fetch('/api/clusters', { cache: 'no-store', signal: AbortSignal.timeout(4000) });
    if (!response.ok) throw new Error('The portal could not refresh the cluster list. Retrying shortly.');
    const snapshot = await response.json();
    clusters = snapshot.clusters;
    const selection = byId('namespace').value;
    byId('namespace').replaceChildren(new Option('All namespaces', ''), ...[...new Set(clusters.map(c => c.namespace))].sort().map(ns => new Option(ns, ns)));
    if ([...byId('namespace').options].some(o => o.value === selection)) byId('namespace').value = selection;
    byId('error').hidden = !snapshot.error;
    byId('error').textContent = snapshot.error || '';
    byId('connection').className = `connection ${snapshot.ready ? 'live' : 'offline'}`;
    byId('connection').replaceChildren(element('i'), document.createTextNode(snapshot.ready ? 'Discovery live' : 'Discovery unavailable'));
    byId('updated').textContent = snapshot.observedAt ? `Updated ${new Date(snapshot.observedAt).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })}` : 'Waiting for discovery';
    render();
  } catch (error) {
    clusters = clusters.map(c => ({ ...c, jobs: null, uiAvailable: false, reason: 'discovery-unavailable' }));
    render();
    byId('error').hidden = false;
    byId('error').textContent = 'The portal could not refresh the cluster list. Showing the last received data; retrying shortly.';
    byId('connection').className = 'connection offline';
    byId('connection').replaceChildren(element('i'), document.createTextNode('Connection lost'));
  } finally { loading = false; byId('refresh').disabled = false; }
}
byId('search').addEventListener('input', render);
byId('namespace').addEventListener('change', render);
byId('refresh').addEventListener('click', refresh);
refresh();
setInterval(refresh, 5000);
