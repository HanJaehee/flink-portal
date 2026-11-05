package io.flinkportal.discovery

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class ClusterResolverTest {
    private val resolver = ClusterResolver()

    @Test
    fun `suspension disables UI even while owned endpoints remain ready`() {
        val f = fixture()
        val result =
            resolver
                .resolve(f.copy(flinks = listOf(f.flinks.single().copy(suspended = true))))
                .single()
        assertEquals("suspended", result.reason)
    }

    @Test
    fun `same name in two namespaces resolves separately`() {
        val a = fixture("a")
        val b = fixture("b")
        val result = resolver.resolve(a + b)
        assertEquals(listOf("a", "b"), result.map { it.key.namespace })
        assertEquals(listOf(8081, 8081), result.map { it.service?.port?.port })
        assertTrue(result.all { it.reason == null })
    }

    @Test
    fun `missing service retains suspended cluster`() {
        val f = fixture().copy(services = emptyList())
        val result =
            resolver.resolve(f.copy(flinks = listOf(f.flinks.single().copy(suspended = true))))
        assertEquals(1, result.size)
        assertEquals("suspended", result.single().reason)
        assertNull(result.single().service)
    }

    @Test
    fun `foreign owner uid and ExternalName are never targets`() {
        val f = fixture()
        for (service in
            listOf(
                f.services.single().copy(ownerUid = "foreign"),
                f.services.single().copy(type = "ExternalName"),
                f.services.single().copy(labels = emptyMap()),
                f.services.single().copy(ports = listOf(RestPort("other", 8081, 8081))),
            )) {
            assertNull(resolver.resolve(f.copy(services = listOf(service))).single().service)
        }
        val recreated = f.copy(flinks = listOf(f.flinks.single().copy(uid = "new-uid")))
        assertNull(resolver.resolve(recreated).single().service)
    }

    @Test
    fun `ambiguous fallback services are unavailable`() {
        val f = fixture()
        val s = f.services.single()
        val result =
            resolver.resolve(f.copy(services = listOf(s.copy(name = "one"), s.copy(name = "two"))))
        assertEquals("ambiguous-service", result.single().reason)
    }

    @Test
    fun `readiness must match service uid port and non terminating endpoint`() {
        val f = fixture()
        for (slice in
            listOf(
                f.slices.single().copy(serviceUid = "old-service"),
                f.slices.single().copy(ports = mapOf("rest" to 9999)),
                f.slices.single().copy(endpoints = listOf(Endpoint(false, false))),
                f.slices.single().copy(endpoints = listOf(Endpoint(true, true))),
            )) {
            assertEquals(
                "no-ready-endpoints",
                resolver.resolve(f.copy(slices = listOf(slice))).single().reason,
            )
        }
        val nilReady = f.slices.single().copy(endpoints = listOf(Endpoint(null, null)))
        assertNull(resolver.resolve(f.copy(slices = listOf(nilReady))).single().reason)
    }

    @Test
    fun `failed job with ready endpoint keeps UI available and uses actual rest port`() {
        val f = fixture()
        val changed =
            f.copy(
                flinks = listOf(f.flinks.single().copy(jobState = "FAILED")),
                services =
                    listOf(f.services.single().copy(ports = listOf(RestPort("rest", 9090, 8081)))),
            )
        val result = resolver.resolve(changed).single()
        assertNull(result.reason)
        assertEquals(9090, result.service?.port?.port)
    }
}

fun fixture(namespace: String = "a"): ResourceSnapshot {
    val key = ClusterKey(namespace, "demo")
    return ResourceSnapshot(
        listOf(FlinkRecord(key, "cr-$namespace", "RUNNING", "v2_3", false)),
        listOf(
            ServiceRecord(
                namespace,
                "demo-rest",
                "svc-$namespace",
                "ClusterIP",
                "dep-$namespace",
                mapOf("app" to "demo", "type" to "flink-native-kubernetes"),
                mapOf("app" to "demo", "component" to "jobmanager"),
                listOf(RestPort("rest", 8081, 8081)),
            )
        ),
        listOf(DeploymentRecord(namespace, "dep-$namespace", "cr-$namespace")),
        listOf(
            SliceRecord(
                namespace,
                "demo-rest",
                "svc-$namespace",
                mapOf("rest" to 8081),
                listOf(Endpoint(true, false)),
            )
        ),
    )
}
