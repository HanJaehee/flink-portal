package io.flinkportal.web

import io.fabric8.kubernetes.client.ConfigBuilder
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import io.flinkportal.TestApi
import io.flinkportal.discovery.*
import org.junit.jupiter.api.Test
import org.springframework.test.web.servlet.client.RestTestClient

class MetadataTest {
    @Test
    fun `source exposes image requests limits and ready manager counts by owner`() {
        TestApi { path, _ ->
            val items =
                when {
                    path.contains("flinkdeployments") ->
                        """{"apiVersion":"flink.apache.org/v1beta1","kind":"FlinkDeployment","metadata":{"name":"demo","namespace":"a","uid":"cr-a"},"spec":{"image":"registry/flink:tag","flinkVersion":"v2_3","jobManager":{"resources":{"requests":{"cpu":"250m","memory":"1Gi"},"limits":{"cpu":"1","memory":"2Gi"}}},"taskManager":{"resource":{"cpu":0.5,"memory":"1024m"}}}}"""
                    path.contains("/deployments") ->
                        """{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"demo","namespace":"a","uid":"dep-a","ownerReferences":[{"apiVersion":"flink.apache.org/v1beta1","kind":"FlinkDeployment","uid":"cr-a"}]},"spec":{"selector":{"matchLabels":{"app":"demo"}},"template":{"spec":{"containers":[]}}},"status":{"readyReplicas":1}}"""
                    path.contains("/services") ->
                        """{"apiVersion":"v1","kind":"Service","metadata":{"name":"demo-rest","namespace":"a","uid":"svc-a","labels":{"app":"demo","type":"flink-native-kubernetes"},"ownerReferences":[{"apiVersion":"apps/v1","kind":"Deployment","uid":"dep-a","controller":true}]},"spec":{"selector":{"app":"demo","component":"jobmanager"},"ports":[{"name":"rest","port":8081,"targetPort":8081}]}}"""
                    path.contains("/pods") ->
                        listOf("dep-a" to true, "foreign" to true, "dep-a" to false)
                            .mapIndexed { index, (owner, ready) ->
                                """{"apiVersion":"v1","kind":"Pod","metadata":{"name":"tm-$index","namespace":"a","labels":{"app":"demo","component":"taskmanager","type":"flink-native-kubernetes"},"ownerReferences":[{"apiVersion":"apps/v1","kind":"Deployment","uid":"$owner","controller":true}]},"spec":{"containers":[]},"status":{"phase":"Running","conditions":[{"type":"Ready","status":"${if (ready) "True" else "False"}"}]}}"""
                            }
                            .joinToString(",")
                    else -> ""
                }
            200 to """{"apiVersion":"v1","kind":"List","metadata":{},"items":[$items]}"""
        }
            .use { api ->
                KubernetesClientBuilder()
                    .withConfig(ConfigBuilder().withMasterUrl(api.url).build())
                    .build()
                    .use { client ->
                        ClusterRegistry(
                                KubernetesSource(client),
                                setOf("a"),
                                ClusterStatusProbe { emptyList() },
                            )
                            .use { registry ->
                                registry.refresh()
                                val web =
                                    RestTestClient.bindToController(PortalRoutes(registry)).build()
                                web.get()
                                    .uri("/api/clusters")
                                    .exchange()
                                    .expectStatus()
                                    .isOk
                                    .expectBody()
                                    .jsonPath("$.clusters[0].image")
                                    .isEqualTo("registry/flink:tag")
                                    .jsonPath("$.clusters[0].jobManager.readyCount")
                                    .isEqualTo(1)
                                    .jsonPath("$.clusters[0].jobManager.requests.cpu")
                                    .isEqualTo("250m")
                                    .jsonPath("$.clusters[0].jobManager.limits.memory")
                                    .isEqualTo("2Gi")
                                    .jsonPath("$.clusters[0].taskManager.readyCount")
                                    .isEqualTo(1)
                                    .jsonPath("$.clusters[0].taskManager.requests.cpu")
                                    .isEqualTo("0.5")
                                    .jsonPath("$.clusters[0].taskManager.limits.cpu")
                                    .isEmpty
                            }
                    }
            }
    }
}
