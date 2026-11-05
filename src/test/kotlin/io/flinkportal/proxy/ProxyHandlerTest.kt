package io.flinkportal.proxy

import com.sun.net.httpserver.HttpServer
import io.fabric8.kubernetes.client.ConfigBuilder
import io.fabric8.kubernetes.client.utils.HttpClientUtils
import io.flinkportal.config.KubernetesClients
import io.flinkportal.discovery.*
import java.net.InetSocketAddress
import java.net.URI
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.http.HttpMethod
import org.springframework.test.web.servlet.client.RestTestClient

class ProxyHandlerTest {
    @Test
    fun `suspended cluster with ready endpoints never probes or proxies`() {
        val resources = fixture()
        val probeCalls = java.util.concurrent.atomic.AtomicInteger()
        ClusterRegistry(
                ResourceSource {
                    resources.copy(
                        flinks = listOf(resources.flinks.single().copy(suspended = true))
                    )
                },
                setOf("a"),
                ClusterStatusProbe {
                    probeCalls.incrementAndGet()
                    emptyList()
                },
            )
            .use { registry ->
                registry.refresh()
                assertEquals(0, probeCalls.get())
                assertTrue(registry.current().ready)
                assertEquals("suspended", registry.current().clusters.single().reason)
                val handler =
                    ProxyHandler(
                        registry,
                        UpstreamTransport { _, _ -> error("must not proxy suspended target") },
                    )
                val web = RestTestClient.bindToController(handler).build()
                web.get().uri("/clusters/a/demo/").exchange().expectStatus().isEqualTo(503)
            }
    }

    @Test
    fun `forwards static and REST reads with safe headers and query`() {
        val seen = AtomicReference<String>()
        server { exchange ->
            seen.set(exchange.requestURI.toString())
            assertNull(exchange.requestHeaders.getFirst("Authorization"))
            assertNull(exchange.requestHeaders.getFirst("Cookie"))
            assertNull(exchange.requestHeaders.getFirst("X-Secret"))
            exchange.responseHeaders.add("Set-Cookie", "session=upstream")
            exchange.responseHeaders.add("Connection", "X-Upstream")
            exchange.responseHeaders.add("X-Upstream", "hidden")
            exchange.responseHeaders.add("Content-Type", "text/plain")
            if (exchange.requestMethod == "HEAD") exchange.sendResponseHeaders(200, -1)
            else {
                exchange.sendResponseHeaders(200, 2)
                exchange.responseBody.use { it.write("ok".toByteArray()) }
            }
            exchange.close()
        }
            .use { upstream ->
                withPortal(upstream.uri) { web, _ ->
                    for (path in
                        listOf(
                            "",
                            "main.js",
                            "jobs/id",
                            "jobmanager/log",
                            "jobs/metrics",
                            "file%20name",
                        )) {
                        web.get()
                            .uri(URI("/clusters/a/demo/$path?get=one%2Ctwo"))
                            .header("Authorization", "Bearer browser")
                            .header("Cookie", "private=yes")
                            .header("Connection", "X-Secret")
                            .header("X-Secret", "no")
                            .exchange()
                            .expectStatus()
                            .isOk
                            .expectHeader()
                            .doesNotExist("Set-Cookie")
                            .expectHeader()
                            .doesNotExist("X-Upstream")
                            .expectBody(String::class.java)
                            .isEqualTo("ok")
                        assertEquals("/$path?get=one%2Ctwo", seen.get())
                    }
                    web.method(HttpMethod.HEAD)
                        .uri("/clusters/a/demo/")
                        .exchange()
                        .expectStatus()
                        .isOk
                        .expectBody()
                        .isEmpty
                    web.get()
                        .uri("/clusters/a/demo")
                        .exchange()
                        .expectStatus()
                        .isTemporaryRedirect
                        .expectHeader()
                        .valueEquals("Location", "/clusters/a/demo/")
                    for (method in
                        listOf(
                            HttpMethod.POST,
                            HttpMethod.DELETE,
                            HttpMethod.PATCH,
                            HttpMethod.PUT,
                        )) {
                        web.method(method)
                            .uri("/clusters/a/demo/jobs/id")
                            .exchange()
                            .expectStatus()
                            .isEqualTo(405)
                    }
                    web.get().uri("/clusters/b/demo/").exchange().expectStatus().isNotFound
                }
            }
    }

    @Test
    fun `unavailable clusters and stale discovery produce 503`() {
        val clock = MutableClock()
        ClusterRegistry(
                ResourceSource { fixture().copy(services = emptyList()) },
                setOf("a"),
                ClusterStatusProbe { emptyList() },
                clock,
            )
            .use { registry ->
                registry.refresh()
                val handler =
                    ProxyHandler(
                        registry,
                        UpstreamTransport { _, _ -> error("must not call offline target") },
                    )
                val web = RestTestClient.bindToController(handler).build()
                web.get().uri("/clusters/a/demo/").exchange().expectStatus().isEqualTo(503)
                clock.now = clock.now.plusSeconds(31)
                web.get().uri("/clusters/a/demo/").exchange().expectStatus().isEqualTo(503)
            }
    }

    @Test
    fun `rewrites internal redirects and rejects external locations and unsafe paths`() {
        val location = AtomicReference("/jobs/id?view=all")
        server { exchange ->
            exchange.responseHeaders.add("Location", location.get())
            exchange.sendResponseHeaders(302, -1)
            exchange.close()
        }
            .use { upstream ->
                withPortal(upstream.uri) { web, _ ->
                    web.get()
                        .uri("/clusters/a/demo/")
                        .exchange()
                        .expectStatus()
                        .isFound
                        .expectHeader()
                        .valueEquals("Location", "/clusters/a/demo/jobs/id?view=all")
                    location.set("details?view=all")
                    web.get()
                        .uri("/clusters/a/demo/jobs/id")
                        .exchange()
                        .expectStatus()
                        .isFound
                        .expectHeader()
                        .valueEquals("Location", "details?view=all")
                    location.set("https://other.example/credentials")
                    web.get().uri("/clusters/a/demo/").exchange().expectStatus().isEqualTo(502)
                    for (path in
                        listOf(
                            "/../secrets",
                            "/%2e%2e/secrets",
                            "/%252e%252e/secrets",
                            "/%2fsecrets",
                            "//other",
                        )) {
                        assertThrows(IllegalArgumentException::class.java) {
                            SafeProxyPath.validate(path)
                        }
                    }
                }
            }
    }

    @Test
    fun `API server transport uses service proxy and kube authentication`() {
        val seen = AtomicReference<String>()
        server { exchange ->
            seen.set(exchange.requestURI.toString())
            assertEquals("Bearer kube-token", exchange.requestHeaders.getFirst("Authorization"))
            exchange.sendResponseHeaders(200, 2)
            exchange.responseBody.use { it.write("ok".toByteArray()) }
        }
            .use { api ->
                val config =
                    ConfigBuilder()
                        .withMasterUrl(api.uri.toString())
                        .withOauthToken("kube-token")
                        .build()
                KubernetesClients.create(config).use { client ->
                    val transport = HttpUpstreamTransport.apiServer(client)
                    val cluster = ClusterResolver().resolve(fixture()).single()
                    val response =
                        transport.exchange(
                            cluster,
                            ForwardRequest("GET", "/jobs/overview", "x=a%2Cb", emptyMap()),
                        )

                    response.close()
                    assertEquals(200, response.status)
                    assertEquals(
                        "/api/v1/namespaces/a/services/http:demo-rest:8081/proxy/jobs/overview?x=a%2Cb",
                        seen.get(),
                    )
                }
            }
    }

    @Test
    fun `authenticated API transport never follows a redirect`() {
        val seen = java.util.concurrent.atomic.AtomicInteger()
        server { exchange ->
            seen.incrementAndGet()
            exchange.sendResponseHeaders(200, -1)
            exchange.close()
        }
            .use { destination ->
                server { exchange ->
                    exchange.responseHeaders.add("Location", destination.uri.toString())
                    exchange.sendResponseHeaders(302, -1)
                    exchange.close()
                }
                    .use { api ->
                        KubernetesClients.create(
                                ConfigBuilder()
                                    .withMasterUrl(api.uri.toString())
                                    .withOauthToken("private")
                                    .build()
                            )
                            .use { client ->
                                val response =
                                    HttpUpstreamTransport.apiServer(client)
                                        .exchange(
                                            ClusterResolver().resolve(fixture()).single(),
                                            ForwardRequest("GET", "/", null, emptyMap()),
                                        )

                                response.close()
                                assertEquals(302, response.status)
                                assertEquals(0, seen.get())
                            }
                    }
            }
    }

    @Test
    fun `response streams before completion and cancellation releases upstream`() {
        val closed = CountDownLatch(1)
        val release = CountDownLatch(1)
        val initial = CountDownLatch(1)
        server { exchange ->
            exchange.sendResponseHeaders(200, 0)
            exchange.responseBody.use {
                it.write("first".toByteArray())
                it.flush()
                initial.countDown()
                release.await(5, TimeUnit.SECONDS)
                runCatching { it.write("last".toByteArray()) }
            }
        }
            .use { upstream ->
                val http = HttpClientUtils.getHttpClientFactory().newBuilder().build()
                http.use {
                    val transport =
                        HttpUpstreamTransport(ClosingHttpClient(http, closed)) { upstream.uri }
                    val response =
                        transport.exchange(
                            ClusterResolver().resolve(fixture()).single(),
                            ForwardRequest("GET", "/", null, emptyMap()),
                        )

                    try {
                        assertTrue(initial.await(1, TimeUnit.SECONDS))
                        assertEquals("first", String(response.body.readNBytes(5)))
                        response.close()
                        assertTrue(
                            closed.await(1, TimeUnit.SECONDS),
                            "disconnect must close the response stream",
                        )
                    } finally {
                        release.countDown()
                        response.close()
                    }
                }
            }
    }

    @Test
    fun `legacy termination and jar plan GETs never reach Flink`() {
        val calls = java.util.concurrent.atomic.AtomicInteger()
        server { exchange ->
            calls.incrementAndGet()
            exchange.sendResponseHeaders(200, -1)
            exchange.close()
        }
            .use { upstream ->
                withPortal(upstream.uri) { web, _ ->
                    val actions =
                        listOf(
                            "jobs/id/yarn-cancel",
                            "jobs/id/yarn-stop",
                            "jars/uploaded.jar/plan",
                            "v1/jobs/id/yarn-cancel/",
                            "v1/jars/uploaded.jar/plan/",
                            "%6aobs/id/yarn-%63ancel",
                            "%761/jobs/id/%79arn-stop/",
                            "v1/%6aars/uploaded.jar/%70lan",
                        )
                    for (action in actions) {
                        web.get()
                            .uri(URI("/clusters/a/demo/$action"))
                            .exchange()
                            .expectStatus()
                            .isForbidden
                        web.method(HttpMethod.HEAD)
                            .uri(URI("/clusters/a/demo/$action"))
                            .exchange()
                            .expectStatus()
                            .isForbidden
                    }
                    assertEquals(0, calls.get())
                }
            }
    }

    @Test
    fun `API proxy forbidden is actionable discovery error rather than offline Flink`() {
        for (status in listOf(401, 403)) {
            io.flinkportal
                .TestApi { _, _ ->
                    status to
                        """{"apiVersion":"v1","kind":"Status","status":"Failure","code":$status}"""
                }
                .use { api ->
                    KubernetesClients.create(
                            ConfigBuilder()
                                .withMasterUrl(api.url)
                                .withOauthToken("secret-token")
                                .build()
                        )
                        .use { client ->
                            val transport = HttpUpstreamTransport.apiServer(client)
                            ClusterRegistry(ResourceSource { fixture() }, setOf("a"), transport)
                                .use { registry ->
                                    registry.refresh()
                                    assertFalse(registry.current().ready)
                                    assertTrue(
                                        registry
                                            .current()
                                            .error
                                            .orEmpty()
                                            .contains("services/proxy")
                                    )
                                    assertFalse(
                                        registry.current().error.orEmpty().contains("secret-token")
                                    )
                                }
                        }
                }
        }
    }

    @Test
    fun `stalled body times out and closes underlying response`() {
        val release = CountDownLatch(1)
        val closed = CountDownLatch(1)
        server { exchange ->
            exchange.sendResponseHeaders(200, 0)
            exchange.responseBody.use {
                it.write("first".toByteArray())
                it.flush()
                release.await(5, TimeUnit.SECONDS)
            }
        }
            .use { upstream ->
                HttpClientUtils.getHttpClientFactory().newBuilder().build().use { http ->
                    val transport =
                        HttpUpstreamTransport(
                            ClosingHttpClient(http, closed),
                            Duration.ofMillis(150),
                        ) {
                            upstream.uri
                        }
                    val response =
                        transport.exchange(
                            ClusterResolver().resolve(fixture()).single(),
                            ForwardRequest("GET", "/", null, emptyMap()),
                        )

                    try {
                        assertEquals("first", String(response.body.readNBytes(5)))
                        assertThrows(java.net.SocketTimeoutException::class.java) {
                            response.body.read()
                        }
                        assertTrue(
                            closed.await(1, TimeUnit.SECONDS),
                            "idle timeout must close the response stream",
                        )
                    } finally {
                        release.countDown()
                        response.close()
                    }
                }
            }
    }

    @Test
    fun `downstream write failure closes upstream without buffering the full response`() {
        val closed = java.util.concurrent.atomic.AtomicBoolean(false)
        val reads = java.util.concurrent.atomic.AtomicInteger()
        val stream =
            object : java.io.InputStream() {
                override fun read(): Int = error("bulk reads expected")

                override fun read(bytes: ByteArray, offset: Int, length: Int): Int {
                    reads.incrementAndGet()
                    bytes[offset] = 1
                    return 1
                }

                override fun close() {
                    closed.set(true)
                }
            }
        ClusterRegistry(
                ResourceSource { fixture() },
                setOf("a"),
                ClusterStatusProbe { emptyList() },
            )
            .use { registry ->
                registry.refresh()
                val handler =
                    ProxyHandler(
                        registry,
                        UpstreamTransport { _, _ ->
                            UpstreamResponse(200, emptyMap(), URI("http://upstream/"), stream)
                        },
                    )
                val request =
                    org.springframework.mock.web.MockHttpServletRequest(
                        "GET",
                        "/clusters/a/demo/log",
                    )
                val output =
                    object : jakarta.servlet.ServletOutputStream() {
                        override fun isReady() = true

                        override fun setWriteListener(listener: jakarta.servlet.WriteListener?) {}

                        override fun write(value: Int) {
                            throw java.io.IOException("browser disconnected")
                        }
                    }
                val response =
                    object : org.springframework.mock.web.MockHttpServletResponse() {
                        override fun getOutputStream() = output

                        override fun isCommitted() = true
                    }
                assertThrows(java.io.IOException::class.java) {
                    handler.handle("a", "demo", request, response)
                }
                assertTrue(closed.get())
                assertEquals(1, reads.get(), "a downstream failure must stop upstream consumption")
            }
    }

    private fun withPortal(uri: URI, block: (RestTestClient, ClusterRegistry) -> Unit) {
        HttpClientUtils.getHttpClientFactory().newBuilder().build().use { http ->
            ClusterRegistry(
                    ResourceSource { fixture() },
                    setOf("a"),
                    ClusterStatusProbe { emptyList() },
                )
                .use { registry ->
                    registry.refresh()
                    val handler = ProxyHandler(registry, HttpUpstreamTransport(http) { uri })
                    block(RestTestClient.bindToController(handler).build(), registry)
                }
        }
    }
}

class LocalServer(val server: HttpServer) : AutoCloseable {
    val uri: URI
        get() = URI("http://127.0.0.1:${server.address.port}/")

    override fun close() {
        server.stop(0)
        (server.executor as? java.util.concurrent.ExecutorService)?.shutdownNow()
    }
}

fun server(handler: (com.sun.net.httpserver.HttpExchange) -> Unit): LocalServer =
    LocalServer(
        HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()
            createContext("/") { exchange ->
                try {
                    handler(exchange)
                } catch (failure: Throwable) {
                    runCatching {
                        exchange.sendResponseHeaders(500, -1)
                        exchange.close()
                    }
                    throw failure
                }
            }
            start()
        }
    )

private class ClosingHttpClient(
    private val delegate: io.fabric8.kubernetes.client.http.HttpClient,
    private val closed: CountDownLatch,
) : io.fabric8.kubernetes.client.http.HttpClient by delegate {
    @Suppress("UNCHECKED_CAST")
    override fun <T : Any?> sendAsync(
        request: io.fabric8.kubernetes.client.http.HttpRequest,
        type: Class<T>,
    ): java.util.concurrent.CompletableFuture<io.fabric8.kubernetes.client.http.HttpResponse<T>> =
        delegate.sendAsync(request, type).thenApply { response ->
            if (type != java.io.InputStream::class.java) response
            else {
                val stream =
                    object : java.io.FilterInputStream(response.body() as java.io.InputStream) {
                        override fun close() {
                            try {
                                super.close()
                            } finally {
                                closed.countDown()
                            }
                        }
                    }
                object : io.fabric8.kubernetes.client.http.HttpResponse<T> by response {
                    override fun body(): T = stream as T
                }
            }
        }
}
