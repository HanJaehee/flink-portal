package io.flinkportal.config

import io.fabric8.kubernetes.client.Config
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import io.fabric8.kubernetes.client.http.HttpClient
import io.fabric8.kubernetes.client.utils.HttpClientUtils

object KubernetesClients {
    fun create(config: Config): KubernetesClient =
        KubernetesClientBuilder()
            .withConfig(config)
            .withHttpClientFactory(
                object : HttpClient.Factory {
                    override fun newBuilder(): HttpClient.Builder =
                        NoRedirectBuilder(HttpClientUtils.getHttpClientFactory().newBuilder())
                }
            )
            .build()

    // Fabric8 applies TLS, kubeconfig authentication and interceptors as usual,
    // but must not follow a Service's redirect using API-server credentials.
    private class NoRedirectBuilder(private val delegate: HttpClient.Builder) :
        HttpClient.Builder by delegate {
        override fun followAllRedirects(): HttpClient.Builder = this
    }
}
