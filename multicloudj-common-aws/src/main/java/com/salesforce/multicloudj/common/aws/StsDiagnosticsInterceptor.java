package com.salesforce.multicloudj.common.aws;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttribute;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.time.Duration;
import java.time.Instant;

public class StsDiagnosticsInterceptor implements ExecutionInterceptor {
    private static final Logger log = LoggerFactory.getLogger(StsDiagnosticsInterceptor.class);
    private static final ExecutionAttribute<Instant> START_TIME = new ExecutionAttribute<>("StartTime");
    private final CloudWatchClient cwClient;

    public StsDiagnosticsInterceptor(CloudWatchClient cwClient) {
        this.cwClient = cwClient;
    }

    @Override
    public void beforeExecution(Context.BeforeExecution context, ExecutionAttributes executionAttributes) {
        executionAttributes.putAttribute(START_TIME, Instant.now());
    }

    @Override
    public void afterExecution(Context.AfterExecution context, ExecutionAttributes executionAttributes) {
        // Correctly passing the SdkRequest
        processCompletion(context.request(), executionAttributes, "SUCCESS", null);
    }

    @Override
    public void onExecutionFailure(Context.FailedExecution context, ExecutionAttributes executionAttributes) {
        // Correctly passing the SdkRequest
        processCompletion(context.request(), executionAttributes, "FAILURE", context.exception().getMessage());
    }

    private void processCompletion(SdkRequest request, ExecutionAttributes executionAttributes, String status, String errorMsg) {
        String action = request.getClass().getSimpleName();
        Instant start = executionAttributes.getAttribute(START_TIME);
        long duration = (start != null) ? Duration.between(start, Instant.now()).toMillis() : 0;

        // FIXED: The type-safe way to check and cast
        String targetRole = "N/A";
        if (request instanceof AssumeRoleRequest) {
            // Because AssumeRoleRequest implements SdkRequest through the STS hierarchy,
            // this cast is valid and safe within this block.
            targetRole = ((AssumeRoleRequest) request).roleArn();
        }

        // 1. STRUCTURED LOGGING
        log.info("STS_COMPLETE | Action: {} | Status: {} | Duration: {}ms | Role: {} | Error: {}",
                action, status, duration, targetRole, (errorMsg != null ? errorMsg : "None"));

        // 2. METRIC PUBLISHING
        publishToCloudWatch(action, status, (double) duration, targetRole);
    }

    private void publishToCloudWatch(String action, String status, Double duration, String roleArn) {
        try {
            // Use a background thread or EMF in high-volume prod to avoid blocking
            MetricDatum count = MetricDatum.builder()
                    .metricName("StsCallCount")
                    .dimensions(d -> d.name("Action").value(action),
                            d -> d.name("Status").value(status))
                    .value(1.0).unit(StandardUnit.COUNT).build();

            cwClient.putMetricData(PutMetricDataRequest.builder()
                    .namespace("SalesforceIntegrations/STS")
                    .metricData(count)
                    .build());
        } catch (Exception e) {
            log.warn("Telemetry Error: Could not publish STS metrics", e);
        }
    }
}