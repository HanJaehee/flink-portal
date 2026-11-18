package io.flinkportal.operator

import io.fabric8.kubernetes.client.ConfigBuilder
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import io.flinkportal.TestApi
import io.flinkportal.config.PortalProperties
import java.util.concurrent.CopyOnWriteArrayList
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.test.web.servlet.client.RestTestClient

class OperatorMonitorTest {
    private val requests = CopyOnWriteArrayList<String>()
    private val deployment =
        """{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"operator","namespace":"ops","uid":"dep","labels":{"app.kubernetes.io/name":"flink-kubernetes-operator","app.kubernetes.io/version":"1.16.1"}},"spec":{"replicas":1,"selector":{"matchLabels":{"app":"operator"}},"template":{"spec":{"containers":[{"name":"operator","image":"operator:1.16.1"}]}}},"status":{"readyReplicas":1}}"""
    private val rs =
        """{"apiVersion":"apps/v1","kind":"ReplicaSet","metadata":{"name":"operator-rs","namespace":"ops","uid":"rs","ownerReferences":[{"apiVersion":"apps/v1","kind":"Deployment","uid":"dep","controller":true}]}}"""

    private fun pod(name: String, owner: String) =
        """{"apiVersion":"v1","kind":"Pod","metadata":{"name":"$name","namespace":"ops","uid":"$name-uid","ownerReferences":[{"apiVersion":"apps/v1","kind":"ReplicaSet","uid":"$owner","controller":true}]},"spec":{"containers":[{"name":"operator","image":"operator:1.16.1"}]},"status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"operator","restartCount":2,"ready":true}]}}"""

    private val cr =
        """{"apiVersion":"flink.apache.org/v1beta1","kind":"FlinkDeployment","metadata":{"name":"demo","namespace":"jobs","uid":"cr","generation":4},"spec":{"job":{}},"status":{"lifecycleState":"UPGRADING","observedGeneration":3,"reconciliationStatus":{"state":"UPGRADING","lastReconciledSpec":"secret-spec"},"jobManagerDeploymentStatus":"DEPLOYING","jobStatus":{"state":"RECONCILING"}}}"""
    private val event =
        """{"apiVersion":"v1","kind":"Event","metadata":{"name":"event","namespace":"jobs"},"involvedObject":{"kind":"FlinkDeployment","name":"demo","namespace":"jobs","uid":"cr"},"type":"Normal","reason":"Submit","message":"Deploying resource","lastTimestamp":"2026-11-18T00:00:00Z","count":1}"""

    private fun list(vararg items: String) =
        """{"apiVersion":"v1","kind":"List","metadata":{},"items":[${items.joinToString(",") }]}"""

    private fun withMonitor(
        enabled: Boolean = true,
        forbidden: Boolean = false,
        logText: String = "recent operator log",
        seriesEvents: Boolean = false,
        forbidEvents: Boolean = false,
        sessionResource: Boolean = false,
        inspect: ((OperatorMonitor) -> Unit)? = null,
        body: (RestTestClient) -> Unit,
    ) {
        TestApi { path, _ ->
            requests.add(path)
            when {
                forbidden ->
                    403 to
                        """{"apiVersion":"v1","kind":"Status","code":403,"message":"Bearer private-token https://private-api"}"""
                path.contains("/log?") -> 200 to logText
                path.contains("/events") && forbidEvents ->
                    403 to """{"apiVersion":"v1","kind":"Status","code":403}"""
                path.contains("/pods/owned") -> 200 to pod("owned", "rs")
                path.contains("/deployments") -> 200 to list(deployment)
                path.contains("/replicasets") -> 200 to list(rs)
                path.contains("/pods") ->
                    200 to list(pod("owned", "rs"), pod("foreign", "other-rs"))
                path.contains("flinkdeployments") ->
                    200 to list(if (sessionResource) cr.replace("\"job\":{}", "") else cr)
                path.contains("/events") ->
                    200 to
                        list(
                            if (seriesEvents)
                                event.replace(
                                    "\"count\":1",
                                    "\"count\":1,\"series\":{\"count\":12,\"lastObservedTime\":\"2026-11-18T01:00:00Z\"}",
                                )
                            else event,
                            event.replace("\"uid\":\"cr\"", "\"uid\":\"deleted-cr\""),
                        )
                else -> 404 to "{}"
            }
        }
            .use { api ->
                KubernetesClientBuilder()
                    .withConfig(ConfigBuilder().withMasterUrl(api.url).build())
                    .build()
                    .use { client ->
                        val properties =
                            PortalProperties(
                                namespaces = "jobs",
                                operatorMonitorEnabled = enabled,
                                operatorNamespaces = "ops",
                            )
                        val monitor = OperatorMonitor(client, properties)
                        inspect?.invoke(monitor)
                        body(RestTestClient.bindToController(OperatorRoutes(monitor)).build())
                    }
            }
    }

    @Test
    fun `disabled monitor never contacts Kubernetes and log endpoint is unavailable`() =
        withMonitor(enabled = false) { web ->
            web.get()
                .uri("/api/operators")
                .exchange()
                .expectStatus()
                .isOk
                .expectBody()
                .jsonPath("$.enabled")
                .isEqualTo(false)
            web.get()
                .uri("/api/operators/ops/operator/logs?pod=owned&container=operator")
                .exchange()
                .expectStatus()
                .isNotFound
            assertTrue(requests.isEmpty())
        }

    @Test
    fun `overview selects safe lifecycle fields and only deployment owned pods and current CR events`() =
        withMonitor { web ->
            web.get()
                .uri("/api/operators")
                .exchange()
                .expectStatus()
                .isOk
                .expectBody()
                .jsonPath("$.operators[0].readyReplicas")
                .isEqualTo(1)
                .jsonPath("$.operators[0].pods.length()")
                .isEqualTo(1)
                .jsonPath("$.operators[0].pods[0].name")
                .isEqualTo("owned")
                .jsonPath("$.operators[0].pods[0].restarts")
                .isEqualTo(2)
                .jsonPath("$.resources[0].lifecycleState")
                .isEqualTo("UPGRADING")
                .jsonPath("$.resources[0].pendingSpec")
                .isEqualTo(true)
                .jsonPath("$.events.length()")
                .isEqualTo(1)
                .consumeWith { assertFalse(String(it.responseBody!!).contains("secret-spec")) }
            assertTrue(
                requests.all { it.contains("/namespaces/ops/") || it.contains("/namespaces/jobs/") }
            )
        }

    @Test
    fun `logs reject other namespaces foreign pods containers and excessive bounds without requesting logs`() =
        withMonitor { web ->
            for (uri in
                listOf(
                    "/api/operators/other/operator/logs?pod=owned&container=operator",
                    "/api/operators/ops/operator/logs?pod=foreign&container=operator",
                    "/api/operators/ops/operator/logs?pod=owned&container=other",
                )) web.get().uri(uri).exchange().expectStatus().isNotFound
            web.get()
                .uri("/api/operators/ops/operator/logs?pod=owned&container=operator&tailLines=2001")
                .exchange()
                .expectStatus()
                .isBadRequest
            assertFalse(requests.any { it.contains("/log?") })
        }

    @Test
    fun `valid logs are bounded timestamped and cannot be cached`() = withMonitor { web ->
        web.get()
            .uri("/api/operators/ops/operator/logs?pod=owned&container=operator&tailLines=200")
            .exchange()
            .expectStatus()
            .isOk
            .expectHeader()
            .valueEquals("Cache-Control", "no-store")
            .expectBody()
            .jsonPath("$.text")
            .isEqualTo("recent operator log")
        val query = requests.single { it.contains("/log?") }
        assertTrue(query.contains("tailLines=200"))
        assertTrue(query.contains("limitBytes=262144"))
        assertTrue(query.contains("sinceSeconds=3600"))
        assertTrue(query.contains("timestamps=true"))
        assertFalse(query.contains("follow=true"))
    }

    @Test
    fun `RBAC failure is explicit and upstream credentials never reach API response`() =
        withMonitor(forbidden = true) { web ->
            web.get()
                .uri("/api/operators")
                .exchange()
                .expectStatus()
                .isOk
                .expectBody()
                .jsonPath("$.errors")
                .isNotEmpty
                .consumeWith {
                    val text = String(it.responseBody!!)
                    assertTrue(text.contains("forbidden"))
                    assertFalse(text.contains("private-token"))
                    assertFalse(text.contains("private-api"))
                }
        }

    @Test
    fun `oversized multibyte logs obey byte limit even if upstream ignores limitBytes`() =
        withMonitor(logText = "한".repeat(200000)) { web ->
            val response =
                web.get()
                    .uri("/api/operators/ops/operator/logs?pod=owned&container=operator")
                    .exchange()
                    .expectStatus()
                    .isOk
                    .expectBody(String::class.java)
                    .returnResult()
                    .responseBody!!
            val text =
                tools.jackson.databind.json.JsonMapper.builder()
                    .build()
                    .readTree(response)["text"]
                    .asString()
            assertTrue(text.toByteArray(Charsets.UTF_8).size <= 262144)
            assertFalse(text.contains('\uFFFD'))
            assertTrue(text.startsWith("한한"))
        }

    @Test
    fun `repeated events use latest series observation time and count`() =
        withMonitor(seriesEvents = true) { web ->
            web.get()
                .uri("/api/operators")
                .exchange()
                .expectStatus()
                .isOk
                .expectBody()
                .jsonPath("$.events[0].at")
                .isEqualTo("2026-11-18T01:00:00Z")
                .jsonPath("$.events[0].count")
                .isEqualTo(12)
        }

    @Test
    fun `event permission failure preserves healthy operator and resource observations`() =
        withMonitor(forbidEvents = true) { web ->
            web.get()
                .uri("/api/operators")
                .exchange()
                .expectStatus()
                .isOk
                .expectBody()
                .jsonPath("$.operators[0].readyReplicas")
                .isEqualTo(1)
                .jsonPath("$.resources[0].lifecycleState")
                .isEqualTo("UPGRADING")
                .jsonPath("$.errors[0].section")
                .isEqualTo("events")
                .jsonPath("$.errors[0].reason")
                .isEqualTo("forbidden")
        }

    @Test
    fun `session clusters do not present placeholder CR job state as managed job activity`() =
        withMonitor(sessionResource = true) { web ->
            web.get()
                .uri("/api/operators")
                .exchange()
                .expectStatus()
                .isOk
                .expectBody()
                .jsonPath("$.resources[0].mode")
                .isEqualTo("SESSION")
                .jsonPath("$.resources[0].jobState")
                .isEmpty
        }

    @Test
    fun `follow logs use bounded initial tail and reject foreign pods before opening stream`() =
        withMonitor(
            inspect = { monitor ->
                assertThrows(org.springframework.web.server.ResponseStatusException::class.java) {
                    monitor.followLogs("ops", "operator", "foreign", "operator", 200)
                }
                monitor.followLogs("ops", "operator", "owned", "operator", 200).use { follow ->
                    assertEquals("recent operator log", follow.output.bufferedReader().readText())
                }
                val query = requests.single { it.contains("/log?") }
                assertTrue(query.contains("follow=true"))
                assertTrue(query.contains("tailLines=200"))
                assertTrue(query.contains("timestamps=true"))
            }
        ) {}

    @Test
    fun `upstream close keeps the follow slot until the downstream session finishes`() =
        withMonitor(
            inspect = { monitor ->
                val streams =
                    (1..4).map { monitor.followLogs("ops", "operator", "owned", "operator", 200) }
                try {
                    streams[0].closeUpstream()
                    streams[0].closeUpstream()
                    val busy =
                        assertThrows(
                            org.springframework.web.server.ResponseStatusException::class.java
                        ) {
                            monitor.followLogs("ops", "operator", "owned", "operator", 200)
                        }
                    assertEquals(429, busy.statusCode.value())
                    streams[0].close()
                    streams[0].close()
                    monitor.followLogs("ops", "operator", "owned", "operator", 200).use {}
                } finally {
                    streams.forEach { it.close() }
                }
            }
        ) {}

    @Test
    fun `watch invalidation refreshes cached observation immediately`() =
        withMonitor(
            inspect = { monitor ->
                val first = monitor.snapshot()
                assertSame(first, monitor.snapshot())
                monitor.invalidate()
                assertNotSame(first, monitor.snapshot())
            }
        ) {}
}
