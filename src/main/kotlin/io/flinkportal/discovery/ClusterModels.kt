package io.flinkportal.discovery

data class ClusterKey(val namespace: String, val name: String) {
    val path: String
        get() = "/clusters/$namespace/$name/"
}

data class FlinkRecord(
    val key: ClusterKey,
    val uid: String,
    val jobState: String,
    val version: String,
    val suspended: Boolean,
    val image: String? = null,
    val jobManager: ManagerResources = ManagerResources(),
    val taskManager: ManagerResources = ManagerResources(),
)

data class ResourceAllocation(val cpu: String? = null, val memory: String? = null)

data class ManagerResources(
    val requests: ResourceAllocation = ResourceAllocation(),
    val limits: ResourceAllocation = ResourceAllocation(),
    val readyCount: Int? = null,
)

data class RestPort(val name: String, val port: Int, val targetPort: Int?)

data class ServiceRecord(
    val namespace: String,
    val name: String,
    val uid: String,
    val type: String,
    val ownerUid: String?,
    val labels: Map<String, String>,
    val selector: Map<String, String>,
    val ports: List<RestPort>,
)

data class DeploymentRecord(
    val namespace: String,
    val uid: String,
    val ownerUid: String?,
    val readyReplicas: Int? = null,
)

data class PodRecord(
    val namespace: String,
    val ownerUid: String?,
    val labels: Map<String, String>,
    val ready: Boolean,
    val terminating: Boolean,
)

data class Endpoint(val ready: Boolean?, val terminating: Boolean?)

data class SliceRecord(
    val namespace: String,
    val serviceName: String,
    val serviceUid: String?,
    val ports: Map<String, Int>,
    val endpoints: List<Endpoint>,
)

data class ResourceSnapshot(
    val flinks: List<FlinkRecord>,
    val services: List<ServiceRecord>,
    val deployments: List<DeploymentRecord>,
    val slices: List<SliceRecord>,
    val pods: List<PodRecord> = emptyList(),
) {
    operator fun plus(other: ResourceSnapshot) =
        ResourceSnapshot(
            flinks + other.flinks,
            services + other.services,
            deployments + other.deployments,
            slices + other.slices,
            pods + other.pods,
        )
}

data class ResolvedService(val name: String, val uid: String, val port: RestPort)

data class FlinkJob(
    val id: String,
    val name: String,
    val state: String,
    val startedAt: String? = null,
    val parallelism: List<Int> = emptyList(),
)

data class ResolvedCluster(
    val flink: FlinkRecord,
    val service: ResolvedService?,
    val reason: String?,
    val jobs: List<FlinkJob>? = null,
) {
    val key: ClusterKey
        get() = flink.key
}
