package io.flinkportal.web

import io.flinkportal.discovery.*
import org.junit.jupiter.api.Test
import org.springframework.test.web.servlet.client.RestTestClient

class PortalRoutesTest {
    @Test
    fun `discovery error is visible unready and does not leak credentials`() {
        ClusterRegistry(
                ResourceSource { error("Bearer private-token https://private-api") },
                setOf("a"),
                ClusterStatusProbe { emptyList() },
            )
            .use { registry ->
                registry.refresh()
                val web = RestTestClient.bindToController(PortalRoutes(registry)).build()
                web.get().uri("/healthz").exchange().expectStatus().isOk
                web.get().uri("/readyz").exchange().expectStatus().isEqualTo(503)
                web.get()
                    .uri("/api/clusters")
                    .exchange()
                    .expectStatus()
                    .isOk
                    .expectBody()
                    .jsonPath("$.ready")
                    .isEqualTo(false)
                    .jsonPath("$.error")
                    .isNotEmpty
                    .consumeWith { check(!String(it.responseBody!!).contains("private-token")) }
            }
    }

    @Test
    fun `offline CR remains visible while discovery itself is ready`() {
        val resources =
            fixture().let {
                it.copy(
                    flinks = listOf(it.flinks.single().copy(suspended = true)),
                    services = emptyList(),
                )
            }
        ClusterRegistry(
                ResourceSource { resources },
                setOf("a"),
                ClusterStatusProbe { emptyList() },
            )
            .use { registry ->
                registry.refresh()
                val web = RestTestClient.bindToController(PortalRoutes(registry)).build()
                web.get().uri("/readyz").exchange().expectStatus().isOk
                web.get()
                    .uri("/api/clusters")
                    .exchange()
                    .expectStatus()
                    .isOk
                    .expectBody()
                    .jsonPath("$.clusters[0].name")
                    .isEqualTo("demo")
                    .jsonPath("$.clusters[0].namespace")
                    .isEqualTo("a")
                    .jsonPath("$.clusters[0].uiAvailable")
                    .isEqualTo(false)
                    .jsonPath("$.clusters[0].reason")
                    .isEqualTo("suspended")
                    .jsonPath("$.clusters[0].uiPath")
                    .isEqualTo("/clusters/a/demo/")
                    .jsonPath("$.clusters[0].service")
                    .doesNotExist()
                    .jsonPath("$.clusters[0].uid")
                    .doesNotExist()
            }
    }
}
