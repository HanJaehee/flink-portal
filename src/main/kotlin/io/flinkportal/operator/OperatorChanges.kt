package io.flinkportal.operator

import io.fabric8.kubernetes.api.model.Event
import io.fabric8.kubernetes.api.model.HasMetadata
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.Informable
import io.fabric8.kubernetes.client.dsl.base.ResourceDefinitionContext
import io.fabric8.kubernetes.client.informers.ResourceEventHandler
import io.fabric8.kubernetes.client.informers.SharedIndexInformer
import io.flinkportal.config.PortalProperties
import jakarta.annotation.PreDestroy
import java.util.concurrent.atomic.AtomicLong
import org.springframework.stereotype.Service

/** One set of reconnecting Kubernetes watches shared by all browser sessions. */
@Service
class OperatorChanges(
    private val client: KubernetesClient,
    private val properties: PortalProperties,
    private val monitor: OperatorMonitor,
) : AutoCloseable {
    private val informers = mutableListOf<SharedIndexInformer<*>>()
    private val revision = AtomicLong()
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
            add(
                client
                    .apps()
                    .deployments()
                    .inNamespace(ns)
                    .withLabel("app.kubernetes.io/name", "flink-kubernetes-operator")
            )
            add(client.apps().replicaSets().inNamespace(ns))
            add(client.pods().inNamespace(ns))
        }
        informers.forEach { it.start() }
    }

    private fun changed() {
        monitor.invalidate()
        revision.incrementAndGet()
    }

    private fun <T : HasMetadata> add(operation: Informable<T>) {
        val informer =
            operation
                .runnableInformer(0)
                .addEventHandler(
                    object : ResourceEventHandler<T> {
                        override fun onAdd(resource: T) = changed()

                        override fun onUpdate(old: T, current: T) {
                            if (old.metadata.resourceVersion != current.metadata.resourceVersion)
                                changed()
                        }

                        override fun onDelete(resource: T, deletedFinalStateUnknown: Boolean) =
                            changed()
                    }
                )
                .exceptionHandler { _, _ ->
                    changed()
                    true
                }
        informers.add(informer)
    }

    fun revision(): Long = revision.get()

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
