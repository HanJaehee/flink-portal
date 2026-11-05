package io.flinkportal.proxy

import io.fabric8.kubernetes.client.utils.HttpClientUtils
import io.flinkportal.TestApi
import io.flinkportal.discovery.*
import io.flinkportal.web.PortalRoutes
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.test.web.servlet.client.RestTestClient

class JobStatusTest {
    @Test
    fun `optional detail reads run concurrently with at most four requests`() {
        val entered = java.util.concurrent.CountDownLatch(4)
        val release = java.util.concurrent.CountDownLatch(1)
        val active = java.util.concurrent.atomic.AtomicInteger()
        val peak = java.util.concurrent.atomic.AtomicInteger()
        val jobs =
            (1..8).joinToString(",") { n ->
                """{"jid":"${n.toString().padStart(32, '0')}","name":"job-$n","state":"RUNNING"}"""
            }
        server { exchange ->
            val body =
                if (exchange.requestURI.path == "/jobs/overview") """{"jobs":[$jobs]}"""
                else {
                    val count = active.incrementAndGet()
                    peak.accumulateAndGet(count, ::maxOf)
                    entered.countDown()
                    try {
                        release.await(4, java.util.concurrent.TimeUnit.SECONDS)
                    } finally {
                        active.decrementAndGet()
                    }
                    """{"vertices":[{"parallelism":2}]}"""
                }
            val bytes = body.toByteArray()
            exchange.sendResponseHeaders(200, bytes.size.toLong())
            exchange.responseBody.use { it.write(bytes) }
        }
            .use { api ->
                HttpClientUtils.getHttpClientFactory().newBuilder().build().use { http ->
                    val result =
                        java.util.concurrent.FutureTask {
                            HttpUpstreamTransport(http) { api.uri }
                                .jobs(ClusterResolver().resolve(fixture()).single())
                        }
                    Thread.ofVirtual().start(result)
                    try {
                        assertTrue(
                            entered.await(1, java.util.concurrent.TimeUnit.SECONDS),
                            "four requests must make progress together",
                        )
                        assertEquals(4, peak.get())
                        release.countDown()
                        val details = result.get(3, java.util.concurrent.TimeUnit.SECONDS)!!
                        assertEquals(8, details.size)
                        assertTrue(details.all { it.parallelism == listOf(2) })
                        assertEquals(4, peak.get())
                    } finally {
                        release.countDown()
                        result.cancel(true)
                    }
                }
            }
    }

    @Test
    fun `slow optional detail headers preserve successful summary within budget`() {
        val release = java.util.concurrent.CountDownLatch(1)
        server { exchange ->
            if (exchange.requestURI.path != "/jobs/overview")
                release.await(5, java.util.concurrent.TimeUnit.SECONDS)
            val body =
                if (exchange.requestURI.path == "/jobs/overview")
                    """{"jobs":[{"jid":"00000000000000000000000000000001","name":"one","state":"RUNNING"}]}"""
                else """{"vertices":[]}"""
            exchange.sendResponseHeaders(200, body.length.toLong())
            exchange.responseBody.use { it.write(body.toByteArray()) }
        }
            .use { api ->
                HttpClientUtils.getHttpClientFactory().newBuilder().build().use { http ->
                    try {
                        val began = System.nanoTime()
                        val jobs =
                            HttpUpstreamTransport(http) { api.uri }
                                .jobs(ClusterResolver().resolve(fixture()).single())
                        assertEquals("RUNNING", jobs?.single()?.state)
                        assertTrue(
                            java.time.Duration.ofNanos(System.nanoTime() - began) <
                                java.time.Duration.ofSeconds(3)
                        )
                    } finally {
                        release.countDown()
                    }
                }
            }
    }

    @Test
    fun `slow optional details preserve summary status within the shared time budget`() {
        val release = java.util.concurrent.CountDownLatch(1)
        server { exchange ->
            val body =
                if (exchange.requestURI.path == "/jobs/overview")
                    """{"jobs":[{"jid":"00000000000000000000000000000001","name":"one","state":"RUNNING"}]}"""
                else "{\"vertices\":["
            exchange.sendResponseHeaders(200, 0)
            exchange.responseBody.use {
                it.write(body.toByteArray())
                it.flush()
                if (exchange.requestURI.path != "/jobs/overview")
                    release.await(5, java.util.concurrent.TimeUnit.SECONDS)
            }
        }
            .use { api ->
                HttpClientUtils.getHttpClientFactory().newBuilder().build().use { http ->
                    try {
                        val began = System.nanoTime()
                        val jobs =
                            HttpUpstreamTransport(http) { api.uri }
                                .jobs(ClusterResolver().resolve(fixture()).single())
                        assertEquals("RUNNING", jobs?.single()?.state)
                        assertTrue(jobs!!.single().parallelism.isEmpty())
                        assertTrue(
                            java.time.Duration.ofNanos(System.nanoTime() - began) <
                                java.time.Duration.ofSeconds(3)
                        )
                    } finally {
                        release.countDown()
                    }
                }
            }
    }

    @Test
    fun `per job start time and vertex parallelism include session jobs and tolerate missing details`() {
        TestApi { path, _ ->
            when (path) {
                "/jobs/overview" ->
                    200 to
                        """{"jobs":[{"jid":"00000000000000000000000000000001","name":"one","state":"RUNNING","start-time":1793887632656},{"jid":"00000000000000000000000000000002","name":"two","state":"RUNNING","start-time":-1}]}"""
                "/jobs/00000000000000000000000000000001" ->
                    200 to
                        """{"vertices":[{"parallelism":1},{"parallelism":4},{"parallelism":4}]}"""
                else -> 404 to "{}"
            }
        }
            .use { api ->
                HttpClientUtils.getHttpClientFactory().newBuilder().build().use { http ->
                    ClusterRegistry(
                            ResourceSource { fixture() },
                            setOf("a"),
                            HttpUpstreamTransport(http) { java.net.URI(api.url) },
                        )
                        .use { registry ->
                            registry.refresh()
                            val web =
                                RestTestClient.bindToController(PortalRoutes(registry)).build()
                            web.get()
                                .uri("/api/clusters")
                                .exchange()
                                .expectBody()
                                .jsonPath("$.clusters[0].uiAvailable")
                                .isEqualTo(true)
                                .jsonPath("$.clusters[0].jobs[0].startedAt")
                                .isEqualTo("2026-11-05T14:07:12.656Z")
                                .jsonPath("$.clusters[0].jobs[0].parallelism[0]")
                                .isEqualTo(1)
                                .jsonPath("$.clusters[0].jobs[0].parallelism[1]")
                                .isEqualTo(4)
                                .jsonPath("$.clusters[0].jobs[1].startedAt")
                                .isEmpty
                                .jsonPath("$.clusters[0].jobs[1].parallelism")
                                .isEmpty
                        }
                }
            }
    }

    @Test
    fun `one REST read publishes all jobs with availability and clears them after failure`() {
        var body =
            """{"jobs":[{"jid":"00000000000000000000000000000001","name":"<script>한글</script>","state":"RUNNING"},{"jid":"00000000000000000000000000000002","name":"batch","state":"FINISHED"},{"jid":"00000000000000000000000000000003","name":"failed job","state":"FAILED"}]}"""
        var calls = 0
        TestApi { path, _ ->
            if (path == "/jobs/overview") {
                calls++
                200 to body
            } else 404 to "{}"
        }
            .use { api ->
                HttpClientUtils.getHttpClientFactory().newBuilder().build().use { http ->
                    val transport = HttpUpstreamTransport(http) { java.net.URI(api.url) }
                    ClusterRegistry(ResourceSource { fixture() }, setOf("a"), transport).use {
                        registry ->
                        registry.refresh()
                        assertEquals(1, calls)
                        val web = RestTestClient.bindToController(PortalRoutes(registry)).build()
                        web.get()
                            .uri("/api/clusters")
                            .exchange()
                            .expectStatus()
                            .isOk
                            .expectBody()
                            .jsonPath("$.clusters[0].uiAvailable")
                            .isEqualTo(true)
                            .jsonPath("$.clusters[0].jobs.length()")
                            .isEqualTo(3)
                            .jsonPath("$.clusters[0].jobs[0].id")
                            .isEqualTo("00000000000000000000000000000001")
                            .jsonPath("$.clusters[0].jobs[0].name")
                            .isEqualTo("<script>한글</script>")
                            .jsonPath("$.clusters[0].jobs[1].state")
                            .isEqualTo("FINISHED")
                            .jsonPath("$.clusters[0].jobs[2].state")
                            .isEqualTo("FAILED")
                        body = "not json"
                        registry.refresh()
                        web.get()
                            .uri("/api/clusters")
                            .exchange()
                            .expectBody()
                            .jsonPath("$.clusters[0].uiAvailable")
                            .isEqualTo(false)
                            .jsonPath("$.clusters[0].jobs")
                            .isEmpty
                    }
                }
            }
    }

    @Test
    fun `job results become unknown when discovery expires`() {
        val clock = MutableClock()
        TestApi { _, _ ->
            200 to
                """{"jobs":[{"jid":"00000000000000000000000000000001","name":"stream","state":"RUNNING"}]}"""
        }
            .use { api ->
                HttpClientUtils.getHttpClientFactory().newBuilder().build().use { http ->
                    ClusterRegistry(
                            ResourceSource { fixture() },
                            setOf("a"),
                            HttpUpstreamTransport(http) { java.net.URI(api.url) },
                            clock,
                        )
                        .use { registry ->
                            registry.refresh()
                            clock.now = clock.now.plusSeconds(31)
                            val web =
                                RestTestClient.bindToController(PortalRoutes(registry)).build()
                            web.get()
                                .uri("/api/clusters")
                                .exchange()
                                .expectBody()
                                .jsonPath("$.clusters[0].uiAvailable")
                                .isEqualTo(false)
                                .jsonPath("$.clusters[0].jobs")
                                .isEmpty
                        }
                }
            }
    }

    @Test
    fun `empty session is available but malformed or oversized job responses are not`() {
        var status = 200
        var body = """{"jobs":[]}"""
        TestApi { _, _ -> status to body }
            .use { api ->
                HttpClientUtils.getHttpClientFactory().newBuilder().build().use { http ->
                    ClusterRegistry(
                            ResourceSource { fixture() },
                            setOf("a"),
                            HttpUpstreamTransport(http) { java.net.URI(api.url) },
                        )
                        .use { registry ->
                            registry.refresh()
                            assertNull(registry.current().clusters.single().reason)
                            for (invalid in
                                listOf(
                                    "{}",
                                    "{\"jobs\":null}",
                                    "{\"jobs\":[{}]}",
                                    "{\"jobs\":[]}" + " ".repeat(4 * 1024 * 1024),
                                )) {
                                body = invalid
                                registry.refresh()
                                assertEquals(
                                    "rest-unreachable",
                                    registry.current().clusters.single().reason,
                                )
                            }
                            body = """{"jobs":[]}"""
                            status = 503
                            registry.refresh()
                            assertEquals(
                                "rest-unreachable",
                                registry.current().clusters.single().reason,
                            )
                        }
                }
            }
    }
}
