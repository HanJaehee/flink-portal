package io.flinkportal.operator

import io.fabric8.kubernetes.api.model.*
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.KubernetesClientException
import io.fabric8.kubernetes.client.dsl.LogWatch
import io.fabric8.kubernetes.client.dsl.base.ResourceDefinitionContext
import io.flinkportal.config.PortalProperties
import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.charset.CodingErrorAction
import java.time.Instant
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.web.server.ResponseStatusException

/** Read-only, optional monitor; failures never change cluster discovery readiness. */
@Service
class OperatorMonitor(
    private val client: KubernetesClient,
    private val properties: PortalProperties,
) {
    private val operatorNamespaces =
        if (properties.operatorMonitorEnabled) properties.operatorNamespaceSet() else emptySet()
    private val namespaces = properties.namespaceSet()
    private val logSlots = Semaphore(4)
    private var cached: OperatorSnapshot? = null
    private var cachedAt = 0L
    private val revision = AtomicLong()
    private var cachedRevision = -1L

    fun invalidate() {
        revision.incrementAndGet()
    }

    private val flinkDefinition =
        ResourceDefinitionContext.Builder()
            .withGroup("flink.apache.org")
            .withVersion("v1beta1")
            .withPlural("flinkdeployments")
            .withKind("FlinkDeployment")
            .withNamespaced(true)
            .build()

    @Synchronized
    fun snapshot(): OperatorSnapshot {
        if (!properties.operatorMonitorEnabled) return OperatorSnapshot(enabled = false)
        val readRevision = revision.get()
        cached?.let {
            if (cachedRevision == readRevision && System.nanoTime() - cachedAt < 5_000_000_000L)
                return it
        }
        val errors = mutableListOf<MonitorError>()
        val operators = operatorNamespaces.flatMap { ns ->
            attempt(errors, ns, "operators") { operatorsIn(ns) }.orEmpty()
        }
        val resources = mutableListOf<ManagedResource>()
        val events = mutableListOf<OperatorEvent>()
        for (ns in namespaces) {
            val crs =
                attempt(errors, ns, "resources") {
                    pages { options ->
                        val op = client.genericKubernetesResources(flinkDefinition)
                        if (ns == "*") op.inAnyNamespace().list(options)
                        else op.inNamespace(ns).list(options)
                    }
                } ?: continue
            resources.addAll(
                crs.map { cr ->
                    val status =
                        cr.additionalProperties["status"] as? Map<*, *> ?: emptyMap<Any, Any>()
                    val spec = cr.additionalProperties["spec"] as? Map<*, *>
                    val observed = (status["observedGeneration"] as? Number)?.toLong()
                    ManagedResource(
                        cr.metadata.namespace,
                        cr.metadata.name,
                        if (spec?.get("job") is Map<*, *>) "APPLICATION" else "SESSION",
                        status["lifecycleState"]?.toString() ?: "UNKNOWN",
                        (status["reconciliationStatus"] as? Map<*, *>)?.get("state")?.toString(),
                        status["jobManagerDeploymentStatus"]?.toString(),
                        if (spec?.get("job") is Map<*, *>)
                            (status["jobStatus"] as? Map<*, *>)?.get("state")?.toString()
                        else null,
                        observed?.let { it < (cr.metadata.generation ?: 0) },
                        status["error"]?.toString()?.take(2000),
                    )
                }
            )
            val current = crs.associateBy { it.metadata.uid }
            attempt(errors, ns, "events") {
                    pages { opts ->
                        val op = client.resources(Event::class.java)
                        if (ns == "*")
                            op.inAnyNamespace()
                                .withField("involvedObject.kind", "FlinkDeployment")
                                .list(opts)
                        else
                            op.inNamespace(ns)
                                .withField("involvedObject.kind", "FlinkDeployment")
                                .list(opts)
                    }
                        .filter { event ->
                            val cr = current[event.involvedObject?.uid]
                            cr != null &&
                                event.involvedObject?.namespace == cr.metadata.namespace &&
                                event.involvedObject?.name == cr.metadata.name &&
                                event.involvedObject?.kind == "FlinkDeployment"
                        }
                        .map { e ->
                            OperatorEvent(
                                e.metadata.namespace,
                                e.involvedObject.name,
                                e.type ?: "Normal",
                                e.reason.orEmpty(),
                                e.message.orEmpty().take(2000),
                                e.series?.lastObservedTime?.time
                                    ?: e.lastTimestamp
                                    ?: e.eventTime?.time
                                    ?: e.metadata.creationTimestamp,
                                e.series?.count ?: e.count ?: 1,
                            )
                        }
                }
                ?.let(events::addAll)
        }
        return OperatorSnapshot(
                true,
                Instant.now().toString(),
                operators,
                resources.sortedWith(compareBy({ it.namespace }, { it.name })),
                events
                    .sortedByDescending { event ->
                        runCatching { Instant.parse(event.at) }.getOrNull()
                    }
                    .take(100),
                errors,
            )
            .also {
                cached = it
                cachedRevision = readRevision
                cachedAt = System.nanoTime()
            }
    }

    private fun operatorsIn(namespace: String): List<OperatorInfo> {
        val deployments = pages {
            client
                .apps()
                .deployments()
                .inNamespace(namespace)
                .withLabel("app.kubernetes.io/name", "flink-kubernetes-operator")
                .list(it)
        }
        val replicaSets = pages { client.apps().replicaSets().inNamespace(namespace).list(it) }
        val pods = pages { client.pods().inNamespace(namespace).list(it) }
        return deployments
            .map { deployment ->
                val ownedSets =
                    replicaSets
                        .filter {
                            controller(it, "Deployment", "apps/v1") == deployment.metadata.uid
                        }
                        .map { it.metadata.uid }
                        .toSet()
                val ownedPods =
                    pods
                        .filter {
                            controller(it, "ReplicaSet", "apps/v1") in ownedSets
                        }
                        .map { pod ->
                            OperatorPod(
                                pod.metadata.name,
                                pod.status?.phase ?: "Unknown",
                                pod.status?.conditions.orEmpty().any {
                                    it.type == "Ready" && it.status == "True"
                                } && pod.metadata.deletionTimestamp == null,
                                pod.status?.containerStatuses.orEmpty().sumOf {
                                    it.restartCount ?: 0
                                },
                                pod.spec?.containers.orEmpty().map { it.name },
                                pod.metadata.uid,
                            )
                        }
                        .sortedBy { it.name }
                OperatorInfo(
                    namespace,
                    deployment.metadata.name,
                    deployment.metadata.labels?.get("app.kubernetes.io/version"),
                    deployment.spec?.template?.spec?.containers.orEmpty().map { it.image },
                    deployment.spec?.replicas ?: 1,
                    deployment.status?.readyReplicas ?: 0,
                    ownedPods,
                )
            }
            .sortedBy { it.name }
    }

    fun logs(
        namespace: String,
        name: String,
        pod: String,
        container: String,
        tailLines: Int,
    ): String {
        validateTarget(namespace, name, pod, container, tailLines)
        acquireLogSlot()
        try {
            checkOwnership(namespace, name, pod, container)
            val request =
                client
                    .pods()
                    .inNamespace(namespace)
                    .withName(pod)
                    .inContainer(container)
                    .usingTimestamps()
                    .limitBytes(262144)
                    .sinceSeconds(3600)
                    .tailingLines(tailLines)
            // Kubernetes limitBytes is approximate. Bound our read as well and discard any
            // incomplete UTF-8 sequence at the byte boundary instead of adding replacement bytes.
            val bytes = request.logInputStream.use { it.readNBytes(262144) }
            return Charsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.IGNORE)
                .onUnmappableCharacter(CodingErrorAction.IGNORE)
                .decode(ByteBuffer.wrap(bytes))
                .toString()
        } catch (e: ResponseStatusException) {
            throw e
        } catch (e: Exception) {
            val forbidden = e is KubernetesClientException && e.code == 403
            throw ResponseStatusException(
                if (forbidden) HttpStatus.FORBIDDEN else HttpStatus.BAD_GATEWAY,
                if (forbidden) "Operator log access forbidden; check Portal RBAC"
                else "Operator logs unavailable",
            )
        } finally {
            logSlots.release()
        }
    }

    private fun validateTarget(
        namespace: String,
        name: String,
        pod: String,
        container: String,
        tailLines: Int,
    ) {
        if (!properties.operatorMonitorEnabled || namespace !in operatorNamespaces) notFound()
        if (tailLines !in 1..2000)
            throw ResponseStatusException(
                HttpStatus.BAD_REQUEST,
                "tailLines must be between 1 and 2000",
            )
        if (
            !listOf(name, pod, container).all {
                it.length <= 253 && it.matches(Regex("[a-z0-9]([-a-z0-9.]*[a-z0-9])?"))
            }
        )
            notFound()
    }

    private fun acquireLogSlot() {
        if (!logSlots.tryAcquire())
            throw ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS, "Too many log requests")
    }

    private fun checkOwnership(namespace: String, name: String, pod: String, container: String) {
        // Re-read ownership for each request: a cached Pod name may have been replaced.
        val operator = operatorsIn(namespace).singleOrNull { it.name == name } ?: notFound()
        val selected =
            operator.pods.singleOrNull { it.name == pod && container in it.containers }
                ?: notFound()
        val current = client.pods().inNamespace(namespace).withName(pod).get() ?: notFound()
        if (current.metadata.uid != selected.uid) notFound()
    }

    fun followLogs(
        namespace: String,
        name: String,
        pod: String,
        container: String,
        tailLines: Int,
    ): OperatorLogFollow {
        validateTarget(namespace, name, pod, container, tailLines)
        acquireLogSlot()
        try {
            checkOwnership(namespace, name, pod, container)
            val watch =
                client
                    .pods()
                    .inNamespace(namespace)
                    .withName(pod)
                    .inContainer(container)
                    .usingTimestamps()
                    .sinceSeconds(3600)
                    .tailingLines(tailLines)
                    .watchLog()
            return OperatorLogFollow(watch) { logSlots.release() }
        } catch (e: Exception) {
            logSlots.release()
            if (e is ResponseStatusException) throw e
            val forbidden = e is KubernetesClientException && e.code == 403
            throw ResponseStatusException(
                if (forbidden) HttpStatus.FORBIDDEN else HttpStatus.BAD_GATEWAY,
                if (forbidden) "Operator log access forbidden; check Portal RBAC"
                else "Operator logs unavailable",
            )
        }
    }

    private fun <T> attempt(
        errors: MutableList<MonitorError>,
        namespace: String,
        section: String,
        read: () -> T,
    ): T? =
        try {
            read()
        } catch (e: Exception) {
            errors.add(
                MonitorError(
                    namespace,
                    section,
                    if (e is KubernetesClientException && e.code == 403) "forbidden"
                    else "unavailable",
                )
            )
            null
        }

    private fun controller(resource: HasMetadata, kind: String, version: String): String? =
        resource.metadata.ownerReferences
            .orEmpty()
            .singleOrNull {
                it.controller == true && it.kind == kind && it.apiVersion == version
            }
            ?.uid

    private fun <T : HasMetadata> pages(
        fetch: (ListOptions) -> KubernetesResourceList<T>
    ): List<T> {
        val items = mutableListOf<T>()
        val seen = mutableSetOf<String>()
        var token: String? = null
        do {
            val page = fetch(ListOptionsBuilder().withLimit(500L).withContinue(token).build())
            items.addAll(page.items)
            check(items.size <= 10000) { "Monitor resource limit exceeded" }
            token = page.metadata?.`continue`?.takeIf { it.isNotBlank() }
            check(token == null || seen.add(token)) { "Kubernetes pagination did not advance" }
        } while (token != null)
        return items
    }

    private fun notFound(): Nothing =
        throw ResponseStatusException(HttpStatus.NOT_FOUND, "Operator or pod not found")
}

data class OperatorSnapshot(
    val enabled: Boolean,
    val observedAt: String? = null,
    val operators: List<OperatorInfo> = emptyList(),
    val resources: List<ManagedResource> = emptyList(),
    val events: List<OperatorEvent> = emptyList(),
    val errors: List<MonitorError> = emptyList(),
)

data class OperatorInfo(
    val namespace: String,
    val name: String,
    val version: String?,
    val images: List<String>,
    val replicas: Int,
    val readyReplicas: Int,
    val pods: List<OperatorPod>,
)

data class OperatorPod(
    val name: String,
    val phase: String,
    val ready: Boolean,
    val restarts: Int,
    val containers: List<String>,
    val uid: String,
)

data class ManagedResource(
    val namespace: String,
    val name: String,
    val mode: String,
    val lifecycleState: String,
    val reconciliationState: String?,
    val jobManagerStatus: String?,
    val jobState: String?,
    val pendingSpec: Boolean?,
    val error: String?,
)

data class OperatorEvent(
    val namespace: String,
    val resource: String,
    val type: String,
    val reason: String,
    val message: String,
    val at: String?,
    val count: Int,
)

data class MonitorError(val namespace: String, val section: String, val reason: String)

class OperatorLogFollow(private val watch: LogWatch, private val release: () -> Unit) :
    AutoCloseable {
    private val upstreamClosed = AtomicBoolean()
    private val closed = AtomicBoolean()
    val output: InputStream
        get() = watch.output

    fun closeUpstream() {
        if (upstreamClosed.compareAndSet(false, true)) watch.close()
    }

    override fun close() {
        if (closed.compareAndSet(false, true))
            try {
                closeUpstream()
            } finally {
                release()
            }
    }
}
