package io.flinkportal.discovery

import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class ClusterRegistryTest {
    @Test
    fun `source and probe time consume snapshot routing lifetime`() {
        val clock = MutableClock()
        val started = clock.now
        ClusterRegistry(
                ResourceSource {
                    clock.now = clock.now.plusSeconds(10)
                    fixture()
                },
                setOf("a"),
                ClusterStatusProbe {
                    clock.now = clock.now.plusSeconds(15)
                    emptyList()
                },
                clock,
            )
            .use { registry ->
                registry.refresh()
                assertTrue(registry.current().ready)
                assertEquals(started, registry.current().observedAt)
                clock.now = started.plusSeconds(31)
                assertThrows(DiscoveryUnavailableException::class.java) {
                    registry.find(ClusterKey("a", "demo"))
                }
            }
    }

    @Test
    fun `expired source read keeps previous list and skips probes`() {
        val clock = MutableClock()
        var slow = false
        val probeCalls = java.util.concurrent.atomic.AtomicInteger()
        ClusterRegistry(
                ResourceSource {
                    if (slow) clock.now = clock.now.plusSeconds(31)
                    if (slow) fixture("b") else fixture()
                },
                setOf("a", "b"),
                ClusterStatusProbe {
                    probeCalls.incrementAndGet()
                    emptyList()
                },
                clock,
            )
            .use { registry ->
                registry.refresh()
                slow = true
                registry.refresh()
                assertEquals(1, probeCalls.get())
                assertEquals("a", registry.current().clusters.single().key.namespace)
                assertFalse(registry.current().ready)
                assertThrows(DiscoveryUnavailableException::class.java) {
                    registry.find(ClusterKey("a", "demo"))
                }
            }
    }

    @Test
    fun `probe sweep expiration prevents publishing and starting another batch`() {
        val clock = MutableClock()
        val resources = (1..5).map { fixture("ns-$it") }.reduce { a, b -> a + b }
        val calls = java.util.concurrent.atomic.AtomicInteger()
        ClusterRegistry(
                ResourceSource { resources },
                setOf("*"),
                ClusterStatusProbe {
                    calls.incrementAndGet()
                    synchronized(clock) { clock.now = clock.now.plusSeconds(10) }
                    emptyList()
                },
                clock,
            )
            .use { registry ->
                registry.refresh()
                assertEquals(4, calls.get())
                assertFalse(registry.current().ready)
                assertTrue(registry.current().clusters.isEmpty())
            }
    }

    @Test
    fun `API failures are unready and stale snapshots cannot route`() {
        val clock = MutableClock()
        var broken = true
        ClusterRegistry(
                ResourceSource { if (broken) error("secret-token") else fixture() },
                setOf("a"),
                ClusterStatusProbe { emptyList() },
                clock,
            )
            .use { registry ->
                registry.refresh()
                assertFalse(registry.current().ready)
                assertFalse(registry.current().error.orEmpty().contains("secret-token"))
                broken = false
                registry.refresh()
                assertTrue(registry.current().ready)
                assertNotNull(registry.find(ClusterKey("a", "demo")))
                clock.now = clock.now.plusSeconds(31)
                assertFalse(registry.current().ready)
                assertThrows(DiscoveryUnavailableException::class.java) {
                    registry.find(ClusterKey("a", "demo"))
                }
            }
    }

    @Test
    fun `later discovery failure keeps last list but disables routing`() {
        var broken = false
        ClusterRegistry(
                ResourceSource { if (broken) error("private") else fixture() },
                setOf("a"),
                ClusterStatusProbe { emptyList() },
            )
            .use { registry ->
                registry.refresh()
                broken = true
                registry.refresh()
                assertEquals(1, registry.current().clusters.size)
                assertFalse(registry.current().ready)
                assertNotNull(registry.current().error)
                assertThrows(DiscoveryUnavailableException::class.java) {
                    registry.find(ClusterKey("a", "demo"))
                }
            }
    }

    @Test
    fun `probe offline and UID replacement preserve list without old target`() {
        var resources = fixture()
        ClusterRegistry(ResourceSource { resources }, setOf("a"), ClusterStatusProbe { null })
            .use { registry ->
                registry.refresh()
                assertEquals("rest-unreachable", registry.current().clusters.single().reason)
                resources =
                    resources.copy(
                        flinks = listOf(resources.flinks.single().copy(uid = "replacement"))
                    )
                registry.refresh()
                assertEquals(1, registry.current().clusters.size)
                assertNull(registry.current().clusters.single().service)
            }
    }

    @Test
    fun `readers see complete snapshots during concurrent refreshes`() {
        val stop = AtomicBoolean(false)
        ClusterRegistry(
                ResourceSource { fixture("a") + fixture("b") },
                setOf("a", "b"),
                ClusterStatusProbe { emptyList() },
            )
            .use { registry ->
                registry.refresh()
                val worker = Executors.newSingleThreadExecutor()
                try {
                    val future = worker.submit {
                        repeat(20) { registry.refresh() }
                        stop.set(true)
                    }
                    while (!stop.get()) assertEquals(2, registry.current().clusters.size)
                    future.get()
                } finally {
                    worker.shutdownNow()
                }
            }
    }
}

class MutableClock(var now: Instant = Instant.parse("2026-01-01T00:00:00Z")) : Clock() {
    override fun instant(): Instant = now

    override fun getZone(): ZoneId = ZoneOffset.UTC

    override fun withZone(zone: ZoneId): Clock = this
}
