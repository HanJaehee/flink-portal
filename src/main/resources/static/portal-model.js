/* Job state and UI availability are independent. A session cluster may match several states. */
export function clusterStates(cluster) {
  if (cluster.reason === 'suspended') return ['SUSPENDED'];
  if (!Array.isArray(cluster.jobs)) return ['UNKNOWN'];
  if (!cluster.jobs.length) return ['NO_JOBS'];
  return [...new Set(cluster.jobs.map(job => job.state || 'UNKNOWN'))];
}
export function matchesCluster(cluster, search, namespace, state) {
  const text = `${cluster.name} ${cluster.namespace} ${(cluster.jobs || []).map(job => `${job.name} ${job.id} ${job.state}`).join(' ')}`;
  return (!namespace || cluster.namespace === namespace) &&
    text.toLowerCase().includes(search.toLowerCase().trim()) &&
    (!state || clusterStates(cluster).includes(state));
}
export function clusterLinks(cluster) {
  const links = { ui: null, metrics: null, logs: null, job: null };
  if (!cluster.uiAvailable || !/^\/clusters\/[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\/[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?\/$/.test(cluster.uiPath || '')) return links;
  links.ui = cluster.uiPath;
  links.metrics = `${cluster.uiPath}#/job-manager/metrics`;
  links.logs = `${cluster.uiPath}#/job-manager/log`;
  const job = cluster.mode === 'APPLICATION' && /^[a-f0-9]{32}$/i.test(cluster.applicationJobId || '')
    ? cluster.jobs?.find(job => job.id === cluster.applicationJobId) : null;
  if (job) {
    const category = ['FINISHED', 'CANCELED', 'FAILED'].includes(job.state) ? 'completed' : 'running';
    links.job = `${cluster.uiPath}#/job/${category}/${job.id}/overview`;
  }
  return links;
}
