package io.flinkportal.web

import io.flinkportal.discovery.ClusterRegistry
import io.flinkportal.discovery.ClusterStatusProbe
import io.flinkportal.discovery.ResourceSource
import io.flinkportal.discovery.fixture
import org.junit.jupiter.api.Test
import org.springframework.test.web.servlet.client.RestTestClient

class MvcMigrationTest {
    @Test
    fun `MVC serves Portal health and discovery through servlet controllers`() {
        ClusterRegistry(
                ResourceSource { fixture() },
                setOf("a"),
                ClusterStatusProbe { emptyList() },
            )
            .use { registry ->
                registry.refresh()
                val web = RestTestClient.bindToController(PortalRoutes(registry)).build()
                web.get().uri("/healthz").exchange().expectStatus().isOk
                web.get()
                    .uri("/api/clusters")
                    .exchange()
                    .expectStatus()
                    .isOk
                    .expectBody()
                    .jsonPath("$.clusters[0].name")
                    .isEqualTo("demo")
            }
    }
}
