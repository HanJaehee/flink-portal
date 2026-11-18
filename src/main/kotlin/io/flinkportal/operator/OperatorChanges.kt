package io.flinkportal.operator

import io.fabric8.kubernetes.api.model.Event
import io.fabric8.kubernetes.api.model.HasMetadata
import io.fabric8.kubernetes.api.model.OwnerReference
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.Informable
import io.fabric8.kubernetes.client.dsl.base.ResourceDefinitionContext
import io.fabric8.kubernetes.client.informers.ResourceEventHandler
import io.fabric8.kubernetes.client.informers.SharedIndexInformer
import io.flinkportal.config.PortalProperties
import jakarta.annotation.PreDestroy
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import org.springframework.stereotype.Service

/** One set of reconnecting Kubernetes watches shared by all browser sessions. */
@Service
class OperatorChanges(
    private val client: KubernetesClient,
    private val properties: PortalProperties,
    private val monitor: OperatorMonitor,
) : AutoCloseable {
    private val informers = mutableListOf<SharedIndexInformer<*>>()
    private val pending = AtomicBoolean()
    private var publishedRevision = 0L
    private var lastPublishedAt: Long? = null
    private var started = false
    private var closed = false

    @Synchronized
    fun start() {
        if (started || closed || !properties.operatorMonitorEnabled) return
        started = true
        val definition =
            ResourceDefinitionContext.Builder()
                .withGroup("flink.apache.org")
                .withVersion("v1beta1")
                .withPlural("flinkdeployments")
                .withKind("FlinkDeployment")
                .withNamespaced(true)
                .build()
        for (ns in properties.namespaceSet()) {
            val cr = client.genericKubernetesResources(definition)
            add(if (ns == "*") cr.inAnyNamespace() else cr.inNamespace(ns))
            val events = client.resources(Event::class.java)
            add(
                if (ns == "*")
                    events.inAnyNamespace().withField("involvedObject.kind", "FlinkDeployment")
                else events.inNamespace(ns).withField("involvedObject.kind", "FlinkDeployment")
            )
        }
        for (ns in properties.operatorNamespaceSet()) {
            val deployments =
                add(
                    client
                        .apps()
                        .deployments()
                        .inNamespace(ns)
                        .withLabel("app.kubernetes.io/name", "flink-kubernetes-operator")
                )
            val replicaSets =
                add(client.apps().replicaSets().inNamespace(ns)) { set ->
                    deployments.owns(set, "Deployment")
                }
            add(client.pods().inNamespace(ns)) { pod ->
                val set = replicaSets.ownerOf(pod, "ReplicaSet")
                set != null && deployments.owns(set, "Deployment")
            }
        }
        informers.forEach { it.start() }
    }

    private fun changed() {
        pending.set(true)
    }

    private fun <T : HasMetadata> add(
        operation: Informable<T>,
        relevant: (T) -> Boolean = { true },
    ): SharedIndexInformer<T> {
        val informer =
            operation
                .runnableInformer(0)
                .addEventHandler(
                    object : ResourceEventHandler<T> {
                        override fun onAdd(resource: T) {
                            if (relevant(resource)) changed()
                        }

                        override fun onUpdate(old: T, current: T) {
                            if (
                                old.metadata.resourceVersion != current.metadata.resourceVersion &&
                                    (relevant(old) || relevant(current))
                            )
                                changed()
                        }

                        override fun onDelete(resource: T, deletedFinalStateUnknown: Boolean) {
                            if (relevant(resource)) changed()
                        }
                    }
                )
                .exceptionHandler { _, _ ->
                    changed()
                    true
                }
        informers.add(informer)
        return informer
    }

    @Synchronized
    fun revision(): Long {
        val now = System.nanoTime()
        val last = lastPublishedAt
        // The SSE loop also checks on idle ticks, so changes queued here are eventually published.
        if (
            (last == null || now - last >= TimeUnit.SECONDS.toNanos(1)) && pending.getAndSet(false)
        ) {
            monitor.invalidate()
            lastPublishedAt = now
            publishedRevision++
        }
        return publishedRevision
    }

    @Synchronized
    fun watching(): Boolean =
        started &&
            !closed &&
            informers.isNotEmpty() &&
            informers.all { it.hasSynced() && it.isWatching }

    @PreDestroy
    @Synchronized
    override fun close() {
        closed = true
        informers.forEach { it.close() }
        informers.clear()
    }
}

private fun <T : HasMetadata> SharedIndexInformer<T>.owns(
    child: HasMetadata,
    kind: String,
): Boolean = ownerOf(child, kind) != null

private fun <T : HasMetadata> SharedIndexInformer<T>.ownerOf(child: HasMetadata, kind: String): T? {
    val ref: OwnerReference =
        child.metadata.ownerReferences.orEmpty().singleOrNull {
            it.controller == true && it.apiVersion == "apps/v1" && it.kind == kind
        } ?: return null
    return store.getByKey("${child.metadata.namespace}/${ref.name}")?.takeIf {
        it.metadata.uid == ref.uid
    }
}
