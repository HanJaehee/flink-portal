package io.flinkportal.discovery

import io.fabric8.kubernetes.api.model.HasMetadata
import io.fabric8.kubernetes.api.model.KubernetesResourceList
import io.fabric8.kubernetes.api.model.ListOptions
import io.fabric8.kubernetes.api.model.ListOptionsBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.base.ResourceDefinitionContext

fun interface ResourceSource {
    fun read(namespaces: Set<String>): ResourceSnapshot
}

class KubernetesSource(private val client: KubernetesClient) : ResourceSource {
    private val definition =
        ResourceDefinitionContext.Builder()
            .withGroup("flink.apache.org")
            .withVersion("v1beta1")
            .withPlural("flinkdeployments")
            .withKind("FlinkDeployment")
            .withNamespaced(true)
            .build()

    override fun read(namespaces: Set<String>): ResourceSnapshot =
        namespaces
            .map { namespace ->
                val all = namespace == "*"
                val flinks = pages { opts ->
                    val op = client.genericKubernetesResources(definition)
                    if (all) op.inAnyNamespace().list(opts)
                    else op.inNamespace(namespace).list(opts)
                }
                    .map { cr ->
                        val spec =
                            cr.additionalProperties["spec"] as? Map<*, *> ?: emptyMap<Any, Any>()
                        val job = spec["job"] as? Map<*, *>
                        val status = cr.additionalProperties["status"] as? Map<*, *>
                        val jobStatus = status?.get("jobStatus") as? Map<*, *>
                        FlinkRecord(
                            ClusterKey(cr.metadata.namespace, cr.metadata.name),
                            cr.metadata.uid,
                            jobStatus?.get("state")?.toString() ?: "UNKNOWN",
                            spec["flinkVersion"]?.toString() ?: "UNKNOWN",
                            job?.get("state") == "suspended",
                            spec["image"]?.toString(),
                            managerResources(spec["jobManager"]),
                            managerResources(spec["taskManager"]),
                        )
                    }
                val services = pages { opts ->
                    if (all) client.services().inAnyNamespace().list(opts)
                    else client.services().inNamespace(namespace).list(opts)
                }
                    .map { svc ->
                        ServiceRecord(
                            svc.metadata.namespace,
                            svc.metadata.name,
                            svc.metadata.uid,
                            svc.spec.type ?: "ClusterIP",
                            owner(svc, "Deployment", "apps/v1"),
                            svc.metadata.labels.orEmpty().toMap(),
                            svc.spec.selector.orEmpty().toMap(),
                            svc.spec.ports
                                .orEmpty()
                                .filter { it.protocol == null || it.protocol == "TCP" }
                                .map {
                                    RestPort(it.name.orEmpty(), it.port, it.targetPort?.intVal)
                                },
                        )
                    }
                val deployments = pages { opts ->
                    if (all) client.apps().deployments().inAnyNamespace().list(opts)
                    else client.apps().deployments().inNamespace(namespace).list(opts)
                }
                    .map {
                        DeploymentRecord(
                            it.metadata.namespace,
                            it.metadata.uid,
                            owner(
                                it,
                                "FlinkDeployment",
                                "flink.apache.org/v1beta1",
                                requireController = false,
                            ),
                            it.status?.readyReplicas,
                        )
                    }
                val slices = pages { opts ->
                    if (all) client.discovery().v1().endpointSlices().inAnyNamespace().list(opts)
                    else client.discovery().v1().endpointSlices().inNamespace(namespace).list(opts)
                }
                    .map { slice ->
                        SliceRecord(
                            slice.metadata.namespace,
                            slice.metadata.labels?.get("kubernetes.io/service-name").orEmpty(),
                            owner(slice, "Service", "v1"),
                            slice.ports
                                .orEmpty()
                                .filter {
                                    it.port != null && (it.protocol == null || it.protocol == "TCP")
                                }
                                .associate { it.name.orEmpty() to it.port },
                            slice.endpoints
                                .orEmpty()
                                .filter { !it.addresses.isNullOrEmpty() }
                                .map { Endpoint(it.conditions?.ready, it.conditions?.terminating) },
                        )
                    }
                val pods = pages { opts ->
                    if (all) client.pods().inAnyNamespace().list(opts)
                    else client.pods().inNamespace(namespace).list(opts)
                }
                    .map { pod ->
                        PodRecord(
                            pod.metadata.namespace,
                            owner(pod, "Deployment", "apps/v1"),
                            pod.metadata.labels.orEmpty().toMap(),
                            pod.status?.phase == "Running" &&
                                pod.status?.conditions.orEmpty().any {
                                    it.type == "Ready" && it.status == "True"
                                },
                            pod.metadata.deletionTimestamp != null,
                        )
                    }
                ResourceSnapshot(flinks, services, deployments, slices, pods)
            }
            .reduce(ResourceSnapshot::plus)

    private fun managerResources(value: Any?): ManagerResources {
        val manager = value as? Map<*, *>
        val resources = (manager?.get("resources") ?: manager?.get("resource")) as? Map<*, *>
        fun allocation(value: Any?): ResourceAllocation {
            val map = value as? Map<*, *>
            return ResourceAllocation(map?.get("cpu")?.toString(), map?.get("memory")?.toString())
        }
        return ManagerResources(
            allocation(resources?.get("requests") ?: resources),
            allocation(resources?.get("limits")),
        )
    }

    private fun owner(
        resource: HasMetadata,
        kind: String,
        apiVersion: String,
        requireController: Boolean = true,
    ): String? =
        resource.metadata.ownerReferences
            .orEmpty()
            .singleOrNull {
                (!requireController || it.controller == true) &&
                    it.kind == kind &&
                    it.apiVersion == apiVersion
            }
            ?.uid

    private fun <T : HasMetadata> pages(
        fetch: (ListOptions) -> KubernetesResourceList<T>
    ): List<T> {
        val items = mutableListOf<T>()
        val seen = mutableSetOf<String>()
        var token: String? = null
        do {
            val page = fetch(ListOptionsBuilder().withLimit(500L).withContinue(token).build())
            items.addAll(page.items)
            token = page.metadata?.`continue`?.takeIf { it.isNotBlank() }
            check(token == null || seen.add(token)) { "Kubernetes pagination did not advance" }
        } while (token != null)
        return items.toList()
    }
}
