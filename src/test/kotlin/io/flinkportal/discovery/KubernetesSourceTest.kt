package io.flinkportal.discovery

import io.fabric8.kubernetes.client.ConfigBuilder
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import io.fabric8.kubernetes.client.KubernetesClientException
import io.flinkportal.TestApi
import java.util.concurrent.CopyOnWriteArrayList
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class KubernetesSourceTest {
    @Test
    fun `scopes reads decodes CR and follows pagination`() {
        val requests = CopyOnWriteArrayList<String>()
        TestApi { path, _ ->
            requests.add(path)
            200 to
                when {
                    path.contains("flinkdeployments") && !path.contains("continue=") ->
                        """{"apiVersion":"flink.apache.org/v1beta1","kind":"FlinkDeploymentList","metadata":{"continue":"next-page"},"items":[{"apiVersion":"flink.apache.org/v1beta1","kind":"FlinkDeployment","metadata":{"name":"demo","namespace":"a","uid":"cr-a"},"spec":{"flinkVersion":"v2_3","job":{"state":"suspended"}},"status":{"jobStatus":{"state":"FINISHED"}}}]}"""
                    else -> """{"apiVersion":"v1","kind":"List","metadata":{},"items":[]}"""
                }
        }
            .use { api ->
                KubernetesClientBuilder()
                    .withConfig(ConfigBuilder().withMasterUrl(api.url).build())
                    .build()
                    .use { client ->
                        val result = KubernetesSource(client).read(setOf("a"))
                        assertEquals("FINISHED", result.flinks.single().jobState)
                        assertTrue(result.flinks.single().suspended)
                        assertEquals("cr-a", result.flinks.single().uid)
                        assertTrue(requests.any { it.contains("continue=next-page") })
                        assertTrue(requests.all { it.contains("/namespaces/a/") })
                    }
            }
    }

    @Test
    fun `native operator non-controller CR owner resolves with actual REST port`() {
        TestApi { path, _ ->
            val item =
                when {
                    path.contains("/pods") -> ""
                    path.contains("flinkdeployments") ->
                        """{"apiVersion":"flink.apache.org/v1beta1","kind":"FlinkDeployment","metadata":{"name":"demo","namespace":"a","uid":"cr-a"},"spec":{"flinkVersion":"v2_3","job":{"state":"running"}},"status":{"jobStatus":{"state":"RUNNING"}}}"""
                    path.contains("endpointslices") ->
                        """{"apiVersion":"discovery.k8s.io/v1","kind":"EndpointSlice","metadata":{"name":"slice","namespace":"a","labels":{"kubernetes.io/service-name":"demo-rest"},"ownerReferences":[{"apiVersion":"v1","kind":"Service","name":"demo-rest","uid":"svc-a","controller":true}]},"addressType":"IPv4","ports":[{"name":"rest","port":8081,"protocol":"TCP"}],"endpoints":[{"addresses":["10.0.0.1"],"conditions":{"ready":true,"terminating":false}}]}"""
                    path.contains("deployments") ->
                        """{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"demo","namespace":"a","uid":"dep-a","ownerReferences":[{"apiVersion":"flink.apache.org/v1beta1","kind":"FlinkDeployment","name":"demo","uid":"cr-a","controller":false}]},"spec":{"selector":{"matchLabels":{"app":"demo"}},"template":{"metadata":{"labels":{"app":"demo"}},"spec":{"containers":[]}}}}"""
                    else ->
                        """{"apiVersion":"v1","kind":"Service","metadata":{"name":"demo-rest","namespace":"a","uid":"svc-a","labels":{"app":"demo","type":"flink-native-kubernetes"},"ownerReferences":[{"apiVersion":"apps/v1","kind":"Deployment","name":"demo","uid":"dep-a","controller":true}]},"spec":{"type":"ClusterIP","selector":{"app":"demo","component":"jobmanager"},"ports":[{"name":"rest","port":9090,"targetPort":8081,"protocol":"TCP"}]}}"""
                }
            200 to """{"apiVersion":"v1","kind":"List","metadata":{},"items":[$item]}"""
        }
            .use { api ->
                KubernetesClientBuilder()
                    .withConfig(ConfigBuilder().withMasterUrl(api.url).build())
                    .build()
                    .use { client ->
                        val result =
                            ClusterResolver()
                                .resolve(KubernetesSource(client).read(setOf("a")))
                                .single()
                        assertNull(result.reason)
                        assertEquals(9090, result.service?.port?.port)
                    }
            }
    }

    @Test
    fun `forbidden and missing CRD are errors rather than empty clusters`() {
        for (status in listOf(403, 404)) {
            TestApi { _, _ ->
                status to
                    """{"kind":"Status","apiVersion":"v1","status":"Failure","message":"not allowed","code":$status}"""
            }
                .use { api ->
                    KubernetesClientBuilder()
                        .withConfig(ConfigBuilder().withMasterUrl(api.url).build())
                        .build()
                        .use { client ->
                            assertThrows(KubernetesClientException::class.java) {
                                KubernetesSource(client).read(setOf("a"))
                            }
                        }
                }
        }
    }
}
