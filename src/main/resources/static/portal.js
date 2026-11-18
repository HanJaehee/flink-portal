import { initializeOperators } from './operators.js';
/* Resource names and status messages are always rendered as text, never markup. */
import { clusterStates, matchesCluster, clusterLinks } from './portal-model.js';
const byId = (id) => document.getElementById(id);
let clusters = [];
let loading = false;
let selectedState = '';
const stateLabel = state => ({ NO_JOBS: 'No jobs', UNKNOWN: 'Unknown' }[state] || state);
const stateTone = state => ['FAILED', 'FAILING'].includes(state) ? 'failed' :
  ['RUNNING', 'FINISHED'].includes(state) ? 'healthy' :
  ['CREATED', 'INITIALIZING', 'RESTARTING', 'RECONCILING', 'CANCELLING', 'SUSPENDED'].includes(state) ? 'pending' : 'neutral';
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
  const visible = clusters.filter(c => matchesCluster(c, search, namespace, selectedState));
  const states = [...new Set(clusters.flatMap(clusterStates))].sort();
  if (selectedState && !states.includes(selectedState)) states.push(selectedState);
  const filters = ['', ...states].map(state => {
    const count = clusters.filter(c => matchesCluster(c, search, namespace, state)).length;
    const button = element('button', `status-filter ${state === selectedState ? 'selected' : ''}`);
    button.type = 'button';
    button.setAttribute('aria-pressed', String(state === selectedState));
    button.append(element('span', '', state ? stateLabel(state) : 'All statuses'), element('span', 'filter-count', count));
    button.addEventListener('click', () => { selectedState = state; render(); });
    return button;
  });
  // Preserve focus while the background refresh replaces filter counts.
  const focusedState = document.activeElement?.closest('#status-filters button')?.dataset.state;
  filters.forEach((button, index) => { button.dataset.state = ['', ...states][index]; });
  byId('status-filters').replaceChildren(...filters);
  if (focusedState !== undefined) filters.find(button => button.dataset.state === focusedState)?.focus();
  const cards = visible.map(c => {
    const card = element('article', 'cluster-card');
    const header = element('div', 'card-heading');
    const identity = element('div', 'cluster-identity');
    identity.append(element('span', 'namespace', c.namespace), element('h3', 'cluster-name', c.name));
    const mode = element('span', 'mode', c.mode === 'APPLICATION' ? 'Application' : c.mode === 'SESSION' ? 'Session' : 'Unknown mode');
    header.append(identity, mode);
    const availability = element('span', `availability ${c.uiAvailable ? '' : 'offline'}`);
    availability.append(element('i', `dot ${c.uiAvailable ? 'green' : 'amber'}`), document.createTextNode(c.uiAvailable ? 'UI available' : (reasonText[c.reason] || 'UI unavailable')));
    const metadata = element('div', 'cluster-meta');
    metadata.append(element('span', 'version', `Apache Flink ${c.version}`), availability);
    const image = element('div', 'cluster-image', c.image || 'Image unavailable');
    image.title = c.image || '';
    const jobs = element('section', 'card-jobs');
    const jobHeading = element('div', 'card-label', `JOBS${Array.isArray(c.jobs) ? ` · ${c.jobs.length}` : ''}`);
    jobs.append(jobHeading);
    if (Array.isArray(c.jobs) && c.jobs.length) {
      const list = element('ul', 'job-list');
      for (const job of c.jobs) {
        const item = element('li', `job-item ${selectedState && job.state === selectedState ? 'matched' : ''}`);
        const line = element('div', 'job-heading');
        line.append(element('span', 'job-name', job.name || '(Unnamed job)'), element('span', `state ${stateTone(job.state)}`, job.state));
        const values = Array.isArray(job.parallelism) ? job.parallelism : [];
        const parallelism = values.length ? (values.length === 1 ? values[0] : `${values[0]}–${values[values.length - 1]}`) : '—';
        const detail = element('div', 'job-detail');
        const parallelismNode = element('span', '', `Parallelism ${parallelism}`);
        parallelismNode.title = 'Parallelism across job vertices; a range means different vertices use different values.';
        detail.append(parallelismNode, element('span', '', `Started ${startedAt(job.startedAt)}`));
        item.append(line, element('span', 'job-id', job.id), detail);
        list.append(item);
      }
      jobs.append(list);
    } else {
      jobs.append(element('span', `state ${stateTone(clusterStates(c)[0])}`, stateLabel(clusterStates(c)[0])));
      if (!Array.isArray(c.jobs) && c.jobState && c.jobState !== 'UNKNOWN') {
        jobs.append(element('span', 'operator-state', `Last operator state: ${c.jobState}`));
      }
    }
    const resources = element('section', 'card-resources');
    const resourceHeading = element('div', 'card-label', 'RESOURCES');
    resourceHeading.title = 'Configured CPU and memory per instance; counts show ready instances.';
    resources.append(resourceHeading, managerRow('JobManager', c.jobManager, c.reason !== 'discovery-unavailable'), managerRow('TaskManager', c.taskManager, c.reason !== 'discovery-unavailable'));
    const actions = element('div', 'card-actions');
    const links = clusterLinks(c);
    const action = (label, href, primary, title) => {
      const link = element(href ? 'a' : 'button', `open ${primary ? 'primary' : ''}`, label);
      if (href) { link.href = href; link.target = '_blank'; link.rel = 'noopener noreferrer'; }
      else { link.type = 'button'; link.disabled = true; }
      link.title = title;
      link.setAttribute('aria-label', `${title} for ${c.namespace}/${c.name}`);
      return link;
    };
    actions.append(action('Open UI ↗', links.ui, true, 'Open Flink UI'), action('Metrics ↗', links.metrics, false, 'JobManager metrics'), action('Logs ↗', links.logs, false, 'JobManager log files'));
    if (c.mode === 'APPLICATION') actions.append(action('Job details ↗', links.job, false, links.job ? 'Open current application job' : 'Current application job is not available'));
    card.append(header, metadata, image, jobs, resources, actions);
    return card;
  });
  byId('clusters').replaceChildren(...cards);
  byId('total').textContent = clusters.length;
  byId('available').textContent = clusters.filter(c => c.uiAvailable).length;
  byId('unavailable').textContent = clusters.filter(c => !c.uiAvailable).length;
  byId('count').textContent = visible.length;
  byId('empty').hidden = visible.length !== 0;
  byId('empty-description').textContent = clusters.length ? 'Try another search, namespace or status filter.' : 'Clusters will appear as they are discovered.';
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
    byId('connection').className = 'connection offline';
    byId('connection').replaceChildren(element('i'), document.createTextNode('Connection lost'));
    byId('error').textContent = 'The portal could not refresh the cluster list. Showing the last received data; retrying shortly.';
  } finally { loading = false; byId('refresh').disabled = false; }
}
byId('search').addEventListener('input', render);
byId('namespace').addEventListener('change', render);
byId('refresh').addEventListener('click', refresh);
refresh();
setInterval(refresh, 5000);

initializeOperators();
