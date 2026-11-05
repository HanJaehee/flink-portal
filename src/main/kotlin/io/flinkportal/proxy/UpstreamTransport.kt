package io.flinkportal.proxy

import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.http.HttpClient
import io.flinkportal.discovery.ClusterStatusProbe
import io.flinkportal.discovery.FlinkJob
import io.flinkportal.discovery.ResolvedCluster
import io.flinkportal.discovery.ServiceProxyAccessException
import java.io.InputStream
import java.net.URI
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper

data class ForwardRequest(
    val method: String,
    val path: String,
    val query: String?,
    val headers: Map<String, List<String>>,
)

class UpstreamResponse(
    val status: Int,
    val headers: Map<String, List<String>>,
    val origin: URI,
    val body: InputStream,
) : AutoCloseable {
    override fun close() = body.close()
}

fun interface UpstreamTransport {
    fun exchange(target: ResolvedCluster, request: ForwardRequest): UpstreamResponse
}

class HttpUpstreamTransport(
    private val http: HttpClient,
    private val bodyIdleTimeout: Duration = Duration.ofSeconds(30),
    private val apiServerHop: Boolean = false,
    private val origin: (ResolvedCluster) -> URI,
) : UpstreamTransport, ClusterStatusProbe {
    private val json = JsonMapper.builder().build()

    init {
        require(!bodyIdleTimeout.isZero && !bodyIdleTimeout.isNegative)
    }

    override fun exchange(target: ResolvedCluster, request: ForwardRequest): UpstreamResponse =
        exchangeWithin(target, request, System.nanoTime() + TimeUnit.SECONDS.toNanos(30))

    private fun exchangeWithin(
        target: ResolvedCluster,
        request: ForwardRequest,
        deadline: Long,
    ): UpstreamResponse {
        require(request.method in setOf("GET", "HEAD"))
        SafeProxyPath.validate(request.path)
        val base = origin(target)
        val uri =
            URI(
                base.toASCIIString().trimEnd('/') +
                    request.path +
                    (request.query?.let { "?$it" } ?: "")
            )
        require(uri.rawFragment == null)
        val remaining = deadline - System.nanoTime()
        require(remaining > 0)
        val builder =
            http
                .newHttpRequestBuilder()
                .uri(uri)
                .method(request.method, null, null as String?)
                .timeout(remaining, TimeUnit.NANOSECONDS)
                .setHeader("Accept-Encoding", "identity")
        HeaderPolicy.requests(request.headers).forEach { (name, values) ->
            values.forEach { builder.header(name, it) }
        }
        val future = http.sendAsync(builder.build(), InputStream::class.java)
        val abandoned = AtomicBoolean(false)
        future.whenComplete { response, _ ->
            if (abandoned.get()) runCatching { response?.body()?.close() }
        }
        try {
            val response = future.get(remaining, TimeUnit.NANOSECONDS)
            return UpstreamResponse(
                response.code(),
                response.headers(),
                base,
                TimedInputStream(response.body(), bodyIdleTimeout),
            )
        } catch (exception: Exception) {
            abandoned.set(true)
            runCatching { future.getNow(null)?.body()?.close() }
            future.cancel(true)
            if (exception is InterruptedException) Thread.currentThread().interrupt()
            throw exception
        }
    }

    private fun readJson(cluster: ResolvedCluster, path: String, deadline: Long): JsonNode? {
        exchangeWithin(cluster, ForwardRequest("GET", path, null, emptyMap()), deadline).use {
            response ->
            if (apiServerHop && response.status in setOf(401, 403))
                throw ServiceProxyAccessException()
            if (response.status != 200) return null
            val expire = StreamTimeouts.schedule(deadline - System.nanoTime()) { response.close() }
            try {
                val bytes = response.body.readNBytes(4 * 1024 * 1024 + 1)
                require(bytes.size <= 4 * 1024 * 1024)
                require(System.nanoTime() < deadline)
                return json.readTree(bytes)
            } finally {
                expire.cancel(false)
            }
        }
    }

    override fun jobs(cluster: ResolvedCluster): List<FlinkJob>? {
        val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(2500)
        return try {
            val root = readJson(cluster, "/jobs/overview", deadline) ?: return null
            val array = root.get("jobs")
            require(array != null && array.isArray)
            val jobs =
                array.values().map { job ->
                    fun text(field: String): String {
                        val value = job.get(field)
                        require(value != null && value.isString)
                        return value.stringValue()
                    }
                    val id = text("jid")
                    require(id.matches(Regex("[0-9a-fA-F]{32}")))
                    val start = job.path("start-time").asLong(-1)
                    FlinkJob(
                        id,
                        text("name"),
                        text("state"),
                        start
                            .takeIf { it > 0 }
                            ?.let { java.time.Instant.ofEpochMilli(it).toString() },
                    )
                }
            if (jobs.isEmpty() || System.nanoTime() >= deadline) return jobs
            // A fixed pool bounds outgoing detail requests; virtual workers keep blocking
            // HTTP and JSON reads straightforward without introducing Reactor.
            val workers = Executors.newFixedThreadPool(4, Thread.ofVirtual().factory())
            val tasks = jobs.map { job ->
                workers.submit<FlinkJob> {
                    try {
                        val details = readJson(cluster, "/jobs/${job.id}", deadline)
                        val vertices = details?.get("vertices")
                        val parallelism =
                            if (vertices?.isArray == true)
                                vertices
                                    .values()
                                    .mapNotNull {
                                        it.path("parallelism").asInt(-1).takeIf { n -> n > 0 }
                                    }
                                    .distinct()
                                    .sorted()
                            else emptyList()
                        job.copy(parallelism = parallelism)
                    } catch (exception: ServiceProxyAccessException) {
                        throw exception
                    } catch (exception: Exception) {
                        job
                    }
                }
            }
            try {
                jobs.zip(tasks).map { (job, task) ->
                    try {
                        if (task.isDone) task.get()
                        else
                            task.get(
                                (deadline - System.nanoTime()).coerceAtLeast(1),
                                TimeUnit.NANOSECONDS,
                            )
                    } catch (exception: java.util.concurrent.ExecutionException) {
                        if (exception.cause is ServiceProxyAccessException) throw exception.cause!!
                        job
                    } catch (exception: InterruptedException) {
                        Thread.currentThread().interrupt()
                        job
                    } catch (exception: Exception) {
                        job
                    }
                }
            } finally {
                tasks.forEach { it.cancel(true) }
                workers.shutdownNow()
            }
        } catch (exception: ServiceProxyAccessException) {
            throw exception
        } catch (exception: Exception) {
            null
        }
    }

    companion object {
        fun apiServer(client: KubernetesClient) =
            HttpUpstreamTransport(client.httpClient, apiServerHop = true) { cluster ->
                val service = requireNotNull(cluster.service)
                URI(
                    client.configuration.masterUrl.trimEnd('/') +
                        "/api/v1/namespaces/${cluster.key.namespace}" +
                        "/services/http:${service.name}:${service.port.port}/proxy/"
                )
            }

        fun direct(http: HttpClient) =
            HttpUpstreamTransport(http) { cluster ->
                val service = requireNotNull(cluster.service)
                URI("http://${service.name}.${cluster.key.namespace}.svc:${service.port.port}/")
            }
    }
}

object SafeProxyPath {
    fun validate(path: String) {
        val lower = path.lowercase()
        require(
            path.startsWith('/') &&
                !path.contains("//") &&
                !path.contains('\\') &&
                !path.contains('?') &&
                !path.contains('#')
        )
        require(path.split('/').none { it == "." || it == ".." })
        require(listOf("%2e", "%2f", "%5c", "%25", "%00", "%0d", "%0a").none { lower.contains(it) })
    }
}

object HeaderPolicy {
    private val hop =
        setOf(
            "connection",
            "keep-alive",
            "proxy-authenticate",
            "proxy-authorization",
            "te",
            "trailer",
            "transfer-encoding",
            "upgrade",
        )
    private val allowedRequests =
        setOf("accept", "range", "if-none-match", "if-modified-since", "if-range")

    private fun blocked(headers: Map<String, List<String>>): Set<String> =
        hop +
            headers.entries
                .filter { it.key.equals("connection", true) }
                .flatMap { it.value }
                .flatMap { it.split(',') }
                .map { it.trim().lowercase() }

    fun requests(headers: Map<String, List<String>>): Map<String, List<String>> =
        headers.filterKeys {
            it.lowercase() in allowedRequests && it.lowercase() !in blocked(headers)
        }

    fun responses(headers: Map<String, List<String>>): Map<String, List<String>> =
        headers.filterKeys {
            it.lowercase() !in blocked(headers) + setOf("set-cookie", "www-authenticate")
        }
}
