package io.flinkportal.discovery

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled

fun interface ClusterStatusProbe {
    fun jobs(cluster: ResolvedCluster): List<FlinkJob>?
}

class DiscoveryUnavailableException : RuntimeException("Cluster discovery is unavailable")

class ServiceProxyAccessException : RuntimeException("Kubernetes Service proxy access denied")

data class DiscoverySnapshot(
    val observedAt: Instant?,
    val clusters: List<ResolvedCluster>,
    val error: String?,
    val ready: Boolean = false,
)

class ClusterRegistry(
    private val source: ResourceSource,
    private val namespaces: Set<String>,
    private val probe: ClusterStatusProbe,
    private val clock: Clock = Clock.systemUTC(),
    private val maxAge: Duration = Duration.ofSeconds(30),
) : AutoCloseable {
    private val log = LoggerFactory.getLogger(javaClass)
    private val state =
        AtomicReference(DiscoverySnapshot(null, emptyList(), "Waiting for Kubernetes discovery"))
    private val resolver = ClusterResolver()
    private val lock = ReentrantLock()
    private val workers = Executors.newFixedThreadPool(4)

    init {
        require(namespaces.isNotEmpty())
    }

    @Scheduled(fixedDelayString = "\${portal.refresh-ms:5000}")
    fun refresh() {
        if (!lock.tryLock()) return
        try {
            val observedAt = clock.instant()
            fun remainingLifetime(): Duration {
                val remaining = maxAge.minus(Duration.between(observedAt, clock.instant()))
                if (remaining.isNegative || remaining.isZero) throw DiscoveryUnavailableException()
                return remaining
            }
            val resolved = resolver.resolve(source.read(namespaces))
            remainingLifetime()
            val checked =
                resolved.chunked(4).flatMap { batch ->
                    val budget = minOf(Duration.ofSeconds(3), remainingLifetime())
                    val futures =
                        workers.invokeAll(
                            batch.map { cluster ->
                                Callable {
                                    if (cluster.reason != null) cluster
                                    else
                                        probe.jobs(cluster)?.let {
                                            cluster.copy(jobs = it.toList())
                                        } ?: cluster.copy(reason = "rest-unreachable")
                                }
                            },
                            budget.toNanos(),
                            TimeUnit.NANOSECONDS,
                        )
                    batch.zip(futures).map { (cluster, future) ->
                        runCatching { future.get() }
                            .getOrElse {
                                if (it.cause is ServiceProxyAccessException) throw it.cause!!
                                cluster.copy(reason = cluster.reason ?: "rest-unreachable")
                            }
                    }
                }
            remainingLifetime()
            state.set(DiscoverySnapshot(observedAt, checked.toList(), null))
        } catch (exception: Exception) {
            if (exception is InterruptedException) Thread.currentThread().interrupt()
            val previous = state.get()
            val message =
                when (exception) {
                    is ServiceProxyAccessException ->
                        "Kubernetes Service proxy access denied. Check credentials and get permission on services/proxy in each configured namespace."
                    is DiscoveryUnavailableException -> "Kubernetes discovery is stale. Retrying."
                    else ->
                        "Kubernetes discovery failed. Check connectivity, context, CRDs and RBAC."
                }
            if (previous.error != message)
                log.warn("Kubernetes discovery failed ({})", exception.javaClass.simpleName)
            state.set(previous.copy(error = message))
        } finally {
            lock.unlock()
        }
    }

    fun current(): DiscoverySnapshot {
        val value = state.get()
        val fresh = value.observedAt?.let { Duration.between(it, clock.instant()) < maxAge } == true
        return value.copy(
            ready = fresh && value.error == null,
            error = value.error ?: if (!fresh) "Kubernetes discovery is stale. Retrying." else null,
        )
    }

    fun find(key: ClusterKey): ResolvedCluster? {
        val value = current()
        if (!value.ready) throw DiscoveryUnavailableException()
        return value.clusters.find { it.key == key }
    }

    override fun close() {
        workers.shutdownNow()
    }
}
