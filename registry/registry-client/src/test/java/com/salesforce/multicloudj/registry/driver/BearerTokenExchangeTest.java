package com.salesforce.multicloudj.registry.driver;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for BearerTokenExchange.
 */
@ExtendWith(MockitoExtension.class)
public class BearerTokenExchangeTest {

    private static final String REGISTRY_ENDPOINT = "https://test-registry.example.com";
    private static final String TOKEN_ENDPOINT = "https://auth.example.com/token";
    private static final String SERVICE = "registry.example.com";
    private static final String REPOSITORY = "test-repo/test-image";
    private static final String IDENTITY_TOKEN = "identity-token-123";

    @Mock
    private CloseableHttpClient mockHttpClient;

    @Mock
    private CloseableHttpResponse mockResponse;

    @Mock
    private StatusLine mockStatusLine;

    private BearerTokenExchange tokenExchange;

    @BeforeEach
    void setUp() {
        tokenExchange = new BearerTokenExchange(mockHttpClient);
    }

    // ==================== getBearerToken() tests ====================

    @Test
    void testGetBearerToken_ThrowsException_WhenChallengeIsNull() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> tokenExchange.getBearerToken(null, IDENTITY_TOKEN, REPOSITORY, "pull"));

        assertTrue(exception.getMessage().contains("Bearer challenge"));
    }

    @Test
    void testGetBearerToken_ThrowsException_WhenChallengeIsNotBearer() {
        AuthChallenge basicChallenge = AuthChallenge.parse("Basic realm=\"test\"");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> tokenExchange.getBearerToken(basicChallenge, IDENTITY_TOKEN, REPOSITORY, "pull"));

        assertTrue(exception.getMessage().contains("Bearer challenge"));
    }

    @Test
    void testGetBearerToken_ThrowsException_WhenRealmIsBlank() {
        // Create a Bearer challenge without realm
        AuthChallenge challengeWithoutRealm = mock(AuthChallenge.class);
        when(challengeWithoutRealm.isBearer()).thenReturn(true);
        when(challengeWithoutRealm.getRealm()).thenReturn(null);

        IOException exception = assertThrows(IOException.class,
                () -> tokenExchange.getBearerToken(challengeWithoutRealm, IDENTITY_TOKEN, REPOSITORY, "pull"));

        assertTrue(exception.getMessage().contains("missing realm"));
    }

    @Test
    void testGetBearerToken_ReturnsToken_FromTokenField() throws IOException {
        AuthChallenge challenge = AuthChallenge.parse(
                "Bearer realm=\"" + TOKEN_ENDPOINT + "\",service=\"" + SERVICE + "\"");

        String tokenResponse = "{\"token\": \"bearer-token-abc\"}";

        when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockResponse);
        when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
        when(mockResponse.getEntity()).thenReturn(new StringEntity(tokenResponse));

        String token = tokenExchange.getBearerToken(challenge, IDENTITY_TOKEN, REPOSITORY, "pull");

        assertEquals("bearer-token-abc", token);
    }

    @Test
    void testGetBearerToken_ReturnsToken_FromAccessTokenField() throws IOException {
        AuthChallenge challenge = AuthChallenge.parse(
                "Bearer realm=\"" + TOKEN_ENDPOINT + "\",service=\"" + SERVICE + "\"");

        String tokenResponse = "{\"access_token\": \"access-token-xyz\"}";

        when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockResponse);
        when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
        when(mockResponse.getEntity()).thenReturn(new StringEntity(tokenResponse));

        String token = tokenExchange.getBearerToken(challenge, IDENTITY_TOKEN, REPOSITORY, "pull");

        assertEquals("access-token-xyz", token);
    }

    @Test
    void testGetBearerToken_ThrowsException_WhenTokenFieldMissing() throws IOException {
        AuthChallenge challenge = AuthChallenge.parse(
                "Bearer realm=\"" + TOKEN_ENDPOINT + "\",service=\"" + SERVICE + "\"");

        String tokenResponse = "{\"expires_in\": 3600}";

        when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockResponse);
        when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
        when(mockResponse.getEntity()).thenReturn(new StringEntity(tokenResponse));

        IOException exception = assertThrows(IOException.class,
                () -> tokenExchange.getBearerToken(challenge, IDENTITY_TOKEN, REPOSITORY, "pull"));

        assertTrue(exception.getMessage().contains("missing token field"));
    }

    @Test
    void testGetBearerToken_ThrowsException_WhenTokenExchangeFails() throws IOException {
        AuthChallenge challenge = AuthChallenge.parse(
                "Bearer realm=\"" + TOKEN_ENDPOINT + "\",service=\"" + SERVICE + "\"");

        String errorResponse = "{\"error\": \"unauthorized\"}";

        when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockResponse);
        when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_UNAUTHORIZED);
        when(mockResponse.getEntity()).thenReturn(new StringEntity(errorResponse));

        IOException exception = assertThrows(IOException.class,
                () -> tokenExchange.getBearerToken(challenge, IDENTITY_TOKEN, REPOSITORY, "pull"));

        assertTrue(exception.getMessage().contains("Token exchange failed"));
        assertTrue(exception.getMessage().contains("401"));
    }

    @Test
    void testGetBearerToken_ThrowsException_WhenInvalidJson() throws IOException {
        AuthChallenge challenge = AuthChallenge.parse(
                "Bearer realm=\"" + TOKEN_ENDPOINT + "\",service=\"" + SERVICE + "\"");

        String invalidJson = "not valid json";

        when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockResponse);
        when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
        when(mockResponse.getEntity()).thenReturn(new StringEntity(invalidJson));

        IOException exception = assertThrows(IOException.class,
                () -> tokenExchange.getBearerToken(challenge, IDENTITY_TOKEN, REPOSITORY, "pull"));

        assertTrue(exception.getMessage().contains("Invalid JSON"));
    }

    @Test
    void testGetBearerToken_BuildsCorrectUrl_WithServiceAndScope() throws IOException {
        AuthChallenge challenge = AuthChallenge.parse(
                "Bearer realm=\"" + TOKEN_ENDPOINT + "\",service=\"" + SERVICE + "\"");

        String tokenResponse = "{\"token\": \"test-token\"}";

        when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockResponse);
        when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
        when(mockResponse.getEntity()).thenReturn(new StringEntity(tokenResponse));

        tokenExchange.getBearerToken(challenge, IDENTITY_TOKEN, REPOSITORY, "pull", "push");

        ArgumentCaptor<HttpGet> requestCaptor = ArgumentCaptor.forClass(HttpGet.class);
        verify(mockHttpClient).execute(requestCaptor.capture());

        String requestUrl = requestCaptor.getValue().getURI().toString();
        assertTrue(requestUrl.contains("service=" + SERVICE));
        assertTrue(requestUrl.contains("scope=repository%3A" + REPOSITORY.replace("/", "%2F") + "%3Apull%2Cpush"));
    }

    @Test
    void testGetBearerToken_UsesChallengeScope_WhenNoRepositoryProvided() throws IOException {
        AuthChallenge challenge = AuthChallenge.parse(
                "Bearer realm=\"" + TOKEN_ENDPOINT + "\",service=\"" + SERVICE + "\",scope=\"repository:default:pull\"");

        String tokenResponse = "{\"token\": \"test-token\"}";

        when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockResponse);
        when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
        when(mockResponse.getEntity()).thenReturn(new StringEntity(tokenResponse));

        tokenExchange.getBearerToken(challenge, IDENTITY_TOKEN, null);

        ArgumentCaptor<HttpGet> requestCaptor = ArgumentCaptor.forClass(HttpGet.class);
        verify(mockHttpClient).execute(requestCaptor.capture());

        String requestUrl = requestCaptor.getValue().getURI().toString();
        assertTrue(requestUrl.contains("scope=repository%3Adefault%3Apull"));
    }

    @Test
    void testGetBearerToken_BuildsUrl_WithoutService() throws IOException {
        // Challenge without service parameter
        AuthChallenge challenge = AuthChallenge.parse("Bearer realm=\"" + TOKEN_ENDPOINT + "\"");

        String tokenResponse = "{\"token\": \"test-token\"}";

        when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockResponse);
        when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
        when(mockResponse.getEntity()).thenReturn(new StringEntity(tokenResponse));

        tokenExchange.getBearerToken(challenge, IDENTITY_TOKEN, REPOSITORY, "pull");

        ArgumentCaptor<HttpGet> requestCaptor = ArgumentCaptor.forClass(HttpGet.class);
        verify(mockHttpClient).execute(requestCaptor.capture());

        String requestUrl = requestCaptor.getValue().getURI().toString();
        // Should start with ? for scope since no service
        assertTrue(requestUrl.startsWith(TOKEN_ENDPOINT + "?scope="));
    }

    @Test
    void testGetBearerToken_HandlesNullTokenValue() throws IOException {
        AuthChallenge challenge = AuthChallenge.parse(
                "Bearer realm=\"" + TOKEN_ENDPOINT + "\",service=\"" + SERVICE + "\"");

        // JSON with null token value
        String tokenResponse = "{\"token\": null}";

        when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockResponse);
        when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
        when(mockResponse.getEntity()).thenReturn(new StringEntity(tokenResponse));

        IOException exception = assertThrows(IOException.class,
                () -> tokenExchange.getBearerToken(challenge, IDENTITY_TOKEN, REPOSITORY, "pull"));

        assertTrue(exception.getMessage().contains("missing token field"));
    }

}
