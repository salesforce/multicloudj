package com.salesforce.multicloudj.registry.aws;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.protocol.HttpContext;

import java.net.URI;

/**
 * HTTP request interceptor that strips Authorization headers when the request target
 * is not the registry host.
 *
 * <p>This is necessary because AWS ECR redirects blob downloads to S3 pre-signed URLs,
 * which already contain authentication in query parameters. Sending an Authorization header to S3
 * causes a 400 error ("Only one auth mechanism allowed").
 */
public class AuthStrippingInterceptor implements HttpRequestInterceptor {
    private final String registryHost;

    /**
     * @param registryEndpoint the registry base URL
     */
    public AuthStrippingInterceptor(String registryEndpoint) {
        this.registryHost = extractHost(registryEndpoint);
    }

    @Override
    public void process(HttpRequest request, HttpContext context) {
        HttpHost targetHost = (HttpHost) context.getAttribute(HttpClientContext.HTTP_TARGET_HOST);
        if (targetHost == null) {
            return;
        }
        if (!registryHost.equalsIgnoreCase(targetHost.getHostName())) {
            request.removeHeaders(HttpHeaders.AUTHORIZATION);
        }
    }

    /**
     * Extracts the hostname from a URL, stripping scheme, port, and path.
     */
    private static String extractHost(String url) {
        return URI.create(url).getHost();
    }
}
