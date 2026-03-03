package com.salesforce.multicloudj.registry.aws;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.ecr.model.AuthorizationData;
import software.amazon.awssdk.services.ecr.model.GetAuthorizationTokenRequest;
import software.amazon.awssdk.services.ecr.model.GetAuthorizationTokenResponse;

import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AwsRegistryTest {

    private static final String TEST_REGION = "us-east-1";
    private static final String TEST_REGISTRY_ENDPOINT = "https://123456789012.dkr.ecr.us-east-1.amazonaws.com";
    private static final String PROVIDER_ID = "aws";
    private static final String AUTH_USERNAME = "AWS";
    private static final String TOKEN_PREFIX = "AWS:";
    private static final String ERR_EMPTY_AUTH_DATA = "ECR returned empty authorization data";
    private static final String ERR_INVALID_TOKEN_FORMAT = "Invalid ECR authorization token format";
    private static final String ERR_FAILED_AUTH_TOKEN = "Failed to get ECR authorization token";
    private static final String AWS_ERROR_CODE_ACCESS_DENIED = "AccessDenied";
    private static final String AWS_ERROR_CODE_UNAVAILABLE = "ServiceUnavailableException";
    private static final long TOKEN_VALIDITY_SECONDS = 43200; // 12 hours

    @FunctionalInterface
    interface RegistryTestAction {
        void execute(AwsRegistry registry) throws Exception;
    }

    private AwsRegistry createRegistryWithMockEcrClient(EcrClient mockEcrClient) {
        AwsRegistry.Builder builder = new AwsRegistry.Builder()
                .withRegion(TEST_REGION)
                .withRegistryEndpoint(TEST_REGISTRY_ENDPOINT);
        builder.providerId(PROVIDER_ID);
        return new AwsRegistry(builder, mockEcrClient);
    }

    private String encodeToken(String token) {
        return Base64.getEncoder().encodeToString((TOKEN_PREFIX + token).getBytes());
    }

    private AuthorizationData authDataWithExpiry(String token, Instant expiresAt) {
        return AuthorizationData.builder()
                .authorizationToken(encodeToken(token))
                .expiresAt(expiresAt)
                .build();
    }

    private GetAuthorizationTokenResponse tokenResponse(AuthorizationData authData) {
        return GetAuthorizationTokenResponse.builder()
                .authorizationData(Collections.singletonList(authData))
                .build();
    }

    private static AwsServiceException awsException(String errorCode) {
        return AwsServiceException.builder()
                .awsErrorDetails(AwsErrorDetails.builder().errorCode(errorCode).build())
                .build();
    }

    static Stream<org.junit.jupiter.params.provider.Arguments> exceptionMappingProvider() { // NOSONAR - needed for @MethodSource resolution
        return Stream.of(
                arguments(new SubstrateSdkException("test"),              SubstrateSdkException.class),
                arguments(awsException(AWS_ERROR_CODE_ACCESS_DENIED),     UnAuthorizedException.class),
                arguments(awsException(AWS_ERROR_CODE_UNAVAILABLE),       UnknownException.class),
                arguments(new IllegalArgumentException("invalid"),        InvalidArgumentException.class),
                arguments(new RuntimeException("unknown"),                UnknownException.class)
        );
    }

    private void withMockedRegistry(RegistryTestAction action) throws Exception {
        EcrClient mockEcrClient = mock(EcrClient.class);
        try (AwsRegistry registry = createRegistryWithMockEcrClient(mockEcrClient)) {
            action.execute(registry);
        }
    }

    @Test
    void testBuilderAndBasicProperties() throws Exception {
        withMockedRegistry(registry -> {
            assertNotNull(registry);
            assertEquals(PROVIDER_ID, registry.getProviderId());
            assertEquals(AUTH_USERNAME, registry.getAuthUsername());
            assertNotNull(registry.builder());
        });
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"   "})
    void testBuilder_InvalidRegion_ThrowsException(String region) {
        assertThrows(InvalidArgumentException.class, () ->
            new AwsRegistry.Builder()
                    .withRegion(region)
                    .withRegistryEndpoint(TEST_REGISTRY_ENDPOINT)
                    .build()
        );
    }

    @Test
    void testGetAuthToken_EmptyAuthorizationData_ThrowsUnknownException() throws Exception {
        EcrClient mockEcrClient = mock(EcrClient.class);
        GetAuthorizationTokenResponse response = GetAuthorizationTokenResponse.builder()
                .authorizationData(Collections.emptyList())
                .build();
        when(mockEcrClient.getAuthorizationToken(any(GetAuthorizationTokenRequest.class))).thenReturn(response);

        try (AwsRegistry registry = createRegistryWithMockEcrClient(mockEcrClient)) {
            UnknownException exception = assertThrows(UnknownException.class, registry::getAuthToken);
            assertEquals(ERR_EMPTY_AUTH_DATA, exception.getMessage());
        }
    }

    @Test
    void testGetAuthToken_InvalidTokenFormat_ThrowsUnknownException() throws Exception {
        EcrClient mockEcrClient = mock(EcrClient.class);
        AuthorizationData authData = AuthorizationData.builder()
                .authorizationToken(Base64.getEncoder().encodeToString("invalidtoken".getBytes()))
                .expiresAt(Instant.now().plusSeconds(TOKEN_VALIDITY_SECONDS))
                .build();
        when(mockEcrClient.getAuthorizationToken(any(GetAuthorizationTokenRequest.class)))
                .thenReturn(tokenResponse(authData));

        try (AwsRegistry registry = createRegistryWithMockEcrClient(mockEcrClient)) {
            UnknownException exception = assertThrows(UnknownException.class, registry::getAuthToken);
            assertEquals(ERR_INVALID_TOKEN_FORMAT, exception.getMessage());
        }
    }

    @Test
    void testGetAuthToken_TokenCachedWithinHalfwayWindow_NoRefresh() throws Exception {
        String expectedToken = "cached-token";
        EcrClient mockEcrClient = mock(EcrClient.class);
        // Token expires 12 hours from now — halfway point is 6 hours from now, so no refresh expected
        when(mockEcrClient.getAuthorizationToken(any(GetAuthorizationTokenRequest.class)))
                .thenReturn(tokenResponse(authDataWithExpiry(expectedToken, Instant.now().plusSeconds(TOKEN_VALIDITY_SECONDS))));

        try (AwsRegistry registry = createRegistryWithMockEcrClient(mockEcrClient)) {
            assertEquals(expectedToken, registry.getAuthToken());
            assertEquals(expectedToken, registry.getAuthToken()); // second call should use cache
            verify(mockEcrClient, times(1)).getAuthorizationToken(any(GetAuthorizationTokenRequest.class));
        }
    }

    @Test
    void testGetAuthToken_TokenPastHalfwayPoint_Refreshes() throws Exception {
        String firstToken = "first-token";
        String refreshedToken = "refreshed-token";
        EcrClient mockEcrClient = mock(EcrClient.class);
        when(mockEcrClient.getAuthorizationToken(any(GetAuthorizationTokenRequest.class)))
                // Token already past halfway point (expired 1 second ago)
                .thenReturn(tokenResponse(authDataWithExpiry(firstToken, Instant.now().minusSeconds(1))))
                .thenReturn(tokenResponse(authDataWithExpiry(refreshedToken, Instant.now().plusSeconds(TOKEN_VALIDITY_SECONDS))));

        try (AwsRegistry registry = createRegistryWithMockEcrClient(mockEcrClient)) {
            registry.getAuthToken(); // primes cache with already-past-halfway token
            assertEquals(refreshedToken, registry.getAuthToken()); // triggers refresh
            verify(mockEcrClient, times(2)).getAuthorizationToken(any(GetAuthorizationTokenRequest.class));
        }
    }

    @Test
    void testGetAuthToken_RefreshFails_FallsBackToCachedToken() throws Exception {
        String cachedToken = "still-valid-token";
        EcrClient mockEcrClient = mock(EcrClient.class);
        when(mockEcrClient.getAuthorizationToken(any(GetAuthorizationTokenRequest.class)))
                // First call primes cache with past-halfway token, second call simulates transient failure
                .thenReturn(tokenResponse(authDataWithExpiry(cachedToken, Instant.now().minusSeconds(1))))
                .thenThrow(awsException(AWS_ERROR_CODE_UNAVAILABLE));

        try (AwsRegistry registry = createRegistryWithMockEcrClient(mockEcrClient)) {
            registry.getAuthToken(); // primes cache
            assertEquals(cachedToken, registry.getAuthToken()); // falls back to cached token
        }
    }

    @Test
    void testGetAuthToken_RefreshFails_NoCachedToken_ThrowsUnknownException() throws Exception {
        EcrClient mockEcrClient = mock(EcrClient.class);
        when(mockEcrClient.getAuthorizationToken(any(GetAuthorizationTokenRequest.class)))
                .thenThrow(awsException(AWS_ERROR_CODE_UNAVAILABLE));

        try (AwsRegistry registry = createRegistryWithMockEcrClient(mockEcrClient)) {
            UnknownException exception = assertThrows(UnknownException.class, registry::getAuthToken);
            assertEquals(ERR_FAILED_AUTH_TOKEN, exception.getMessage());
        }
    }

    @ParameterizedTest
    @MethodSource("exceptionMappingProvider")
    void testGetException(Throwable input, Class<? extends SubstrateSdkException> expected) throws Exception {
        withMockedRegistry(registry -> assertEquals(expected, registry.getException(input)));
    }
}
