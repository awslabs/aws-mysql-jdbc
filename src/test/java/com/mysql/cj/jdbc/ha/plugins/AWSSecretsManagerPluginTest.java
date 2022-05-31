/*
 * AWS JDBC Driver for MySQL
 * Copyright Amazon.com Inc. or affiliates.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package com.mysql.cj.jdbc.ha.plugins;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.jdbc.JdbcPropertySetImpl;
import com.mysql.cj.jdbc.ha.ConnectionProxy;
import com.mysql.cj.log.Log;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;
import software.amazon.awssdk.utils.Pair;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AWSSecretsManagerPluginTest {

  private static final String TEST_REGION = "us-east-2";
  private static final String TEST_SECRET_ID = "secretId";
  private static final String TEST_USERNAME = "testUser";
  private static final String TEST_PASSWORD = "testPassword";
  private static final String VALID_SECRET_STRING = "{\"username\": \"" + TEST_USERNAME + "\", \"password\": \"" + TEST_PASSWORD + "\"}";
  private static final String INVALID_SECRET_STRING = "{username: invalid, password: invalid}";
  private static final Pair<String, Region> SECRET_CACHE_KEY = Pair.of(TEST_SECRET_ID, Region.of(TEST_REGION));
  private static final AWSSecretsManagerPlugin.Secret SECRET = new AWSSecretsManagerPlugin.Secret("testUser", "testPassword");
  private static final String TEST_DB_URL = "jdbc:mysql:aws://test-domain:3306/test";
  private static final String TEST_SQL_ERROR = "SQL exception error message";
  private static final String SQL_STATE_FAIL_GENERIC = "HY000";
  private static final GetSecretValueResponse VALID_GET_SECRET_VALUE_RESPONSE = GetSecretValueResponse.builder().secretString(VALID_SECRET_STRING).build();
  private static final GetSecretValueResponse INVALID_GET_SECRET_VALUE_RESPONSE = GetSecretValueResponse.builder().secretString(INVALID_SECRET_STRING).build();
  private final PropertySet propertySet = new JdbcPropertySetImpl();
  private AWSSecretsManagerPlugin plugin;
  private AutoCloseable closeable;
  private ConnectionUrl connectionUrl;

  @Mock ConnectionProxy proxy;
  @Mock IConnectionPlugin nextPlugin;
  @Mock Log logger;
  @Mock SecretsManagerClient mockSecretsManagerClient;
  @Mock GetSecretValueRequest mockGetValueRequest;
  @Captor ArgumentCaptor<ConnectionUrl> captor;

  @BeforeEach
  private void init() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);

    Properties properties = new Properties();
    properties.setProperty(AWSSecretsManagerPlugin.SECRET_ID_PROPERTY, TEST_SECRET_ID);
    properties.setProperty(AWSSecretsManagerPlugin.REGION_PROPERTY, TEST_REGION);
    this.propertySet.initializeProperties(properties);

    this.connectionUrl = ConnectionUrl.getConnectionUrlInstance(TEST_DB_URL, properties);

    this.plugin = new AWSSecretsManagerPlugin(
        proxy,
        propertySet,
        nextPlugin,
        logger,
        mockSecretsManagerClient,
        mockGetValueRequest);
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
    AWSSecretsManagerPlugin.SECRET_CACHE.clear();
  }

  /**
   * The plugin will successfully open a connection with a cached secret.
   */
  @Test
  public void testConnectWithCachedSecrets() throws SQLException {
    // Add initial cached secret to be used for a connection
    AWSSecretsManagerPlugin.SECRET_CACHE.put(SECRET_CACHE_KEY, SECRET);

    this.plugin.openInitialConnection(this.connectionUrl);

    assertEquals(1, AWSSecretsManagerPlugin.SECRET_CACHE.size());
    verify(this.mockSecretsManagerClient, never()).getSecretValue(this.mockGetValueRequest);
    verify(this.nextPlugin).openInitialConnection(this.captor.capture());
    final List<ConnectionUrl> connectionUrls = this.captor.getAllValues();
    final Map<String, String> connectionProperties = connectionUrls.get(0).getOriginalProperties();
    assertEquals(TEST_USERNAME, connectionProperties.get("user"));
    assertEquals(TEST_PASSWORD, connectionProperties.get("password"));
  }

  /**
   * The plugin will attempt to open a connection with a cached secret, but it will fail with a generic SQL exception.
   * In this case, the plugin will rethrow the error back to the user.
   */
  @Test
  public void testFailedInitialConnectionWithGenericError() throws SQLException {
    final SQLException failedFirstConnectionGenericException = new SQLException(TEST_SQL_ERROR, SQL_STATE_FAIL_GENERIC);
    doThrow(failedFirstConnectionGenericException).when(this.nextPlugin).openInitialConnection(any(ConnectionUrl.class));

    final SQLException connectionFailedException = assertThrows(SQLException.class, () -> this.plugin.openInitialConnection(this.connectionUrl));

    assertEquals(TEST_SQL_ERROR, connectionFailedException.getMessage());
    assertEquals(0, AWSSecretsManagerPlugin.SECRET_CACHE.size());
    verify(this.mockSecretsManagerClient, never()).getSecretValue(this.mockGetValueRequest);
    verify(this.nextPlugin).openInitialConnection(this.captor.capture());
    final List<ConnectionUrl> connectionUrls = this.captor.getAllValues();
    final Map<String, String> connectionProperties = connectionUrls.get(0).getOriginalProperties();
    assertNull(connectionProperties.get("user"));
    assertNull(connectionProperties.get("password"));
  }

  /**
   * The plugin will attempt to open a connection with a cached secret, but it will fail with an access error. In
   * this case, the plugin will fetch the secret and will retry the connection.
   */
  @Test
  public void testConnectWithNewSecrets() throws SQLException {
    // Fail initial connection attempt so secrets will be retrieved. Second attempt should be successful
    final SQLException failedFirstConnectionAccessException  = new SQLException(TEST_SQL_ERROR, AWSSecretsManagerPlugin.SQLSTATE_ACCESS_ERROR);
    doThrow(failedFirstConnectionAccessException).doNothing().when(nextPlugin).openInitialConnection(any(ConnectionUrl.class));
    when(this.mockSecretsManagerClient.getSecretValue(this.mockGetValueRequest)).thenReturn(VALID_GET_SECRET_VALUE_RESPONSE);

    this.plugin.openInitialConnection(this.connectionUrl);

    assertEquals(1, AWSSecretsManagerPlugin.SECRET_CACHE.size());
    verify(this.mockSecretsManagerClient).getSecretValue(this.mockGetValueRequest);

    // Verify the openInitialConnection method was called using different ConnectionUrl arguments
    verify(this.nextPlugin, times(2)).openInitialConnection(this.captor.capture());
    final List<ConnectionUrl> connectionUrls = this.captor.getAllValues();
    Map<String, String> connectionPropsWithCachedSecret = connectionUrls.get(0).getOriginalProperties();
    Map<String, String> connectionPropsWithNewSecret = connectionUrls.get(1).getOriginalProperties();
    assertNotEquals(TEST_USERNAME, connectionPropsWithCachedSecret.get("user"));
    assertNotEquals(TEST_PASSWORD, connectionPropsWithCachedSecret.get("password"));
    assertEquals(TEST_USERNAME, connectionPropsWithNewSecret.get("user"));
    assertEquals(TEST_PASSWORD, connectionPropsWithNewSecret.get("password"));
  }

  /**
   * The plugin will attempt to open a connection with a cached secret, but it will fail with an access error. In
   * this case, the plugin will attempt to fetch the secret and retry the connection, but it will fail because the
   * returned secret could not be parsed.
   */
  @Test
  public void testFailedToReadSecrets() throws SQLException {
    // Fail initial connection attempt so secrets will be retrieved
    final SQLException failedFirstConnectionAccessException  = new SQLException(TEST_SQL_ERROR, AWSSecretsManagerPlugin.SQLSTATE_ACCESS_ERROR);
    doThrow(failedFirstConnectionAccessException).doNothing().when(this.nextPlugin).openInitialConnection(any(ConnectionUrl.class));
    when(this.mockSecretsManagerClient.getSecretValue(this.mockGetValueRequest)).thenReturn(INVALID_GET_SECRET_VALUE_RESPONSE);

    final SQLException readSecretsFailedException = assertThrows(SQLException.class, () -> this.plugin.openInitialConnection(this.connectionUrl));

    assertEquals(readSecretsFailedException.getMessage(), AWSSecretsManagerPlugin.ERROR_GET_SECRETS_FAILED);
    assertEquals(0, AWSSecretsManagerPlugin.SECRET_CACHE.size());
    verify(this.mockSecretsManagerClient).getSecretValue(this.mockGetValueRequest);
    verify(this.nextPlugin).openInitialConnection(this.captor.capture());
    final List<ConnectionUrl> connectionUrls = this.captor.getAllValues();
    final Map<String, String> connectionProperties = connectionUrls.get(0).getOriginalProperties();
    assertNull(connectionProperties.get("user"));
    assertNull(connectionProperties.get("password"));
  }

  /**
   * The plugin will attempt to open a connection with a cached secret, but it will fail with an access error. In
   * this case, the plugin will attempt to fetch the secret and retry the connection, but it will fail because an
   * exception was thrown by the AWS Secrets Manager.
   */
  @Test
  public void testFailedToGetSecrets() throws SQLException {
    // Fail initial connection attempt so secrets will be retrieved
    final SQLException failedFirstConnectionAccessException  = new SQLException(TEST_SQL_ERROR, AWSSecretsManagerPlugin.SQLSTATE_ACCESS_ERROR);
    doThrow(failedFirstConnectionAccessException).doNothing().when(nextPlugin).openInitialConnection(any(ConnectionUrl.class));
    doThrow(SecretsManagerException.class).when(this.mockSecretsManagerClient).getSecretValue(this.mockGetValueRequest);

    final SQLException getSecretsFailedException = assertThrows(SQLException.class, () -> this.plugin.openInitialConnection(this.connectionUrl));

    assertEquals(getSecretsFailedException.getMessage(), AWSSecretsManagerPlugin.ERROR_GET_SECRETS_FAILED);
    assertEquals(0, AWSSecretsManagerPlugin.SECRET_CACHE.size());
    verify(this.mockSecretsManagerClient).getSecretValue(this.mockGetValueRequest);
    verify(this.nextPlugin).openInitialConnection(this.captor.capture());
    final List<ConnectionUrl> connectionUrls = this.captor.getAllValues();
    final Map<String, String> connectionProperties = connectionUrls.get(0).getOriginalProperties();
    assertNull(connectionProperties.get("user"));
    assertNull(connectionProperties.get("password"));
  }
}
