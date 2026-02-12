package com.salesforce.multicloudj.registry.driver;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Unit tests for AuthChallenge.
 */
@ExtendWith(MockitoExtension.class)
public class AuthChallengeTest {

    private static final String REGISTRY_ENDPOINT = "https://test-registry.example.com";
    private static final String TOKEN_ENDPOINT = "https://auth.example.com/token";
    private static final String SERVICE = "registry.example.com";

    @Mock
    private CloseableHttpClient mockHttpClient;

    @Mock
    private CloseableHttpResponse mockResponse;

    @Mock
    private StatusLine mockStatusLine;

    // ==================== discover() tests ====================

    @Test
    void testDiscover_ReturnsAnonymous_WhenStatusOk() throws IOException {
        when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockResponse);
        when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);

        AuthChallenge challenge = AuthChallenge.discover(mockHttpClient, REGISTRY_ENDPOINT);

        assertNotNull(challenge);
        assertEquals("Anonymous", challenge.getScheme());
    }

    @Test
    void testDiscover_ParsesWwwAuthenticate_WhenUnauthorized() throws IOException {
        String wwwAuthHeader = "Bearer realm=\"" + TOKEN_ENDPOINT + "\",service=\"" + SERVICE + "\"";

        when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockResponse);
        when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_UNAUTHORIZED);
        when(mockResponse.containsHeader(HttpHeaders.WWW_AUTHENTICATE)).thenReturn(true);
        when(mockResponse.getFirstHeader(HttpHeaders.WWW_AUTHENTICATE))
                .thenReturn(new BasicHeader(HttpHeaders.WWW_AUTHENTICATE, wwwAuthHeader));

        AuthChallenge challenge = AuthChallenge.discover(mockHttpClient, REGISTRY_ENDPOINT);

        assertNotNull(challenge);
        assertEquals("Bearer", challenge.getScheme());
        assertEquals(TOKEN_ENDPOINT, challenge.getRealm());
        assertEquals(SERVICE, challenge.getService());
    }

    @Test
    void testDiscover_ThrowsException_WhenUnauthorizedWithoutHeader() throws IOException {
        when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockResponse);
        when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_UNAUTHORIZED);
        when(mockResponse.containsHeader(HttpHeaders.WWW_AUTHENTICATE)).thenReturn(false);

        IOException exception = assertThrows(IOException.class,
                () -> AuthChallenge.discover(mockHttpClient, REGISTRY_ENDPOINT));

        assertTrue(exception.getMessage().contains("401 without WWW-Authenticate"));
    }

    @Test
    void testDiscover_ThrowsException_WhenUnexpectedStatus() throws IOException {
        when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockResponse);
        when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_INTERNAL_SERVER_ERROR);

        IOException exception = assertThrows(IOException.class,
                () -> AuthChallenge.discover(mockHttpClient, REGISTRY_ENDPOINT));

        assertTrue(exception.getMessage().contains("Unexpected response"));
        assertTrue(exception.getMessage().contains("500"));
    }

    // ==================== anonymous() tests ====================

    @Test
    void testAnonymous_ReturnsAnonymousChallenge() {
        AuthChallenge challenge = AuthChallenge.anonymous();

        assertNotNull(challenge);
        assertEquals("Anonymous", challenge.getScheme());
        assertNull(challenge.getRealm());
        assertNull(challenge.getService());
        assertNull(challenge.getScope());
        assertFalse(challenge.isBearer());
    }

    // ==================== parse() tests ====================

    @Test
    void testParse_BearerChallenge_WithAllParams() {
        String header = "Bearer realm=\"https://auth.example.com/token\",service=\"registry.example.com\",scope=\"repository:test:pull\"";

        AuthChallenge challenge = AuthChallenge.parse(header);

        assertNotNull(challenge);
        assertEquals("Bearer", challenge.getScheme());
        assertEquals("https://auth.example.com/token", challenge.getRealm());
        assertEquals("registry.example.com", challenge.getService());
        assertEquals("repository:test:pull", challenge.getScope());
        assertTrue(challenge.isBearer());
    }

    @Test
    void testParse_BasicChallenge() {
        String header = "Basic realm=\"Registry Realm\"";

        AuthChallenge challenge = AuthChallenge.parse(header);

        assertNotNull(challenge);
        assertEquals("Basic", challenge.getScheme());
        assertEquals("Registry Realm", challenge.getRealm());
        assertNull(challenge.getService());
        assertNull(challenge.getScope());
        assertFalse(challenge.isBearer());
    }

    @Test
    void testParse_CaseInsensitiveScheme() {
        String header = "bearer realm=\"https://auth.example.com/token\"";

        AuthChallenge challenge = AuthChallenge.parse(header);

        assertNotNull(challenge);
        assertEquals("bearer", challenge.getScheme());
        assertTrue(challenge.isBearer());
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"   ", "\t", "\n"})
    void testParse_ThrowsException_WhenHeaderIsBlank(String header) {
        assertThrows(IllegalArgumentException.class, () -> AuthChallenge.parse(header));
    }

    @Test
    void testParse_ThrowsException_WhenUnsupportedScheme() {
        String header = "Digest realm=\"test\"";

        UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class,
                () -> AuthChallenge.parse(header));

        assertTrue(exception.getMessage().contains("Unsupported authentication scheme"));
    }

    @Test
    void testParse_HandlesEmptyParams() {
        String header = "Bearer";

        AuthChallenge challenge = AuthChallenge.parse(header);

        assertNotNull(challenge);
        assertEquals("Bearer", challenge.getScheme());
        assertNull(challenge.getRealm());
        assertNull(challenge.getService());
        assertNull(challenge.getScope());
    }

}
