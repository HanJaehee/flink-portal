package io.flinkportal.discovery

class ClusterResolver {
    fun resolve(resources: ResourceSnapshot): List<ResolvedCluster> =
        resources.flinks.sortedWith(compareBy({ it.key.namespace }, { it.key.name })).map { flink ->
            val owned =
                resources.services.filter { service ->
                    service.namespace == flink.key.namespace &&
                        service.type != "ExternalName" &&
                        service.labels["app"] == flink.key.name &&
                        service.labels["type"] == "flink-native-kubernetes" &&
                        service.selector["app"] == flink.key.name &&
                        service.selector["component"] == "jobmanager" &&
                        resources.deployments.any {
                            it.namespace == service.namespace &&
                                it.uid == service.ownerUid &&
                                it.ownerUid == flink.uid
                        } &&
                        service.ports.count { it.name == "rest" && it.port in 1..65535 } == 1
                }
            val conventional = owned.filter { it.name == "${flink.key.name}-rest" }
            val candidates = conventional.ifEmpty { owned }
            if (candidates.size != 1) {
                ResolvedCluster(
                    flink,
                    null,
                    when {
                        candidates.size > 1 -> "ambiguous-service"
                        flink.suspended -> "suspended"
                        else -> "missing-service"
                    },
                )
            } else {
                val service = candidates.single()
                val deployment =
                    resources.deployments.single {
                        it.namespace == service.namespace && it.uid == service.ownerUid
                    }
                val described =
                    flink.copy(
                        jobManager =
                            flink.jobManager.copy(readyCount = deployment.readyReplicas ?: 0),
                        taskManager =
                            flink.taskManager.copy(
                                readyCount =
                                    resources.pods.count {
                                        it.namespace == service.namespace &&
                                            it.ownerUid == deployment.uid &&
                                            it.labels["app"] == flink.key.name &&
                                            it.labels["type"] == "flink-native-kubernetes" &&
                                            it.labels["component"] == "taskmanager" &&
                                            it.ready &&
                                            !it.terminating
                                    }
                            ),
                    )
                val port = service.ports.single { it.name == "rest" && it.port in 1..65535 }
                val ready =
                    resources.slices.any { slice ->
                        slice.namespace == service.namespace &&
                            slice.serviceName == service.name &&
                            slice.serviceUid == service.uid &&
                            slice.ports[port.name]?.let {
                                it in 1..65535 && (port.targetPort == null || port.targetPort == it)
                            } == true &&
                            slice.endpoints.any { it.ready != false && it.terminating != true }
                    }
                ResolvedCluster(
                    described,
                    ResolvedService(service.name, service.uid, port),
                    when {
                        flink.suspended -> "suspended"
                        ready -> null
                        else -> "no-ready-endpoints"
                    },
                )
            }
        }
}
