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
import com.mysql.cj.log.Log;
import com.mysql.cj.util.LRUCache;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;
import software.amazon.awssdk.utils.Pair;

import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.Callable;

public class AWSSecretsManagerPlugin implements IConnectionPlugin {
  static final String SECRET_ID_PROPERTY = "secretsManagerSecretId";
  static final String REGION_PROPERTY = "secretsManagerRegion";
  private static final String ERROR_MISSING_DEPENDENCY =
      "[AWSSecretsManagerPlugin] Required dependency 'AWS Java SDK for AWS Secrets Manager' is not on the classpath";
  static final String ERROR_GET_SECRETS_FAILED =
      "[AWSSecretsManagerPlugin] Was not able to either fetch or read the database credentials from AWS Secrets Manager. Ensure the correct secretId and region properties have been provided";
  static final String SQLSTATE_ACCESS_ERROR = "28000";

  static final LRUCache<Pair<String, Region>, Secret> SECRET_CACHE = new LRUCache<>(100);

  private final IConnectionPlugin nextPlugin;
  private final Log logger;
  private final String secretId;
  private final Region region;
  private final SecretsManagerClient secretsManagerClient;
  private final GetSecretValueRequest getSecretValueRequest;

  public AWSSecretsManagerPlugin(
      ICurrentConnectionProvider currentConnectionProvider,
      PropertySet propertySet,
      IConnectionPlugin nextPlugin,
      Log logger) throws SQLException {

    this(
        currentConnectionProvider,
        propertySet,
        nextPlugin,
        logger,
        null,
        null
    );
  }

  AWSSecretsManagerPlugin(
      ICurrentConnectionProvider currentConnectionProvider,
      PropertySet propertySet,
      IConnectionPlugin nextPlugin,
      Log logger,
      SecretsManagerClient secretsManagerClient,
      GetSecretValueRequest getSecretValueRequest) throws SQLException {

    try {
      Class.forName("software.amazon.awssdk.services.secretsmanager.SecretsManagerClient");
    } catch (ClassNotFoundException e) {
      logger.logError(ERROR_MISSING_DEPENDENCY);
      throw new SQLException(ERROR_MISSING_DEPENDENCY);
    }

    this.nextPlugin = nextPlugin;
    this.logger = logger;
    this.secretId = propertySet.getStringProperty(SECRET_ID_PROPERTY).getValue();
    this.region = Region.of(propertySet.getStringProperty(REGION_PROPERTY).getValue());

    if (secretsManagerClient != null && getSecretValueRequest != null) {
      this.secretsManagerClient = secretsManagerClient;
      this.getSecretValueRequest = getSecretValueRequest;

    } else {
      this.secretsManagerClient = SecretsManagerClient.builder()
          .region(this.region)
          .build();
      this.getSecretValueRequest = GetSecretValueRequest.builder()
          .secretId(this.secretId)
          .build();
    }
  }

  @Override
  public void openInitialConnection(ConnectionUrl connectionUrl) throws SQLException {
    final Pair<String, Region> secretKey = Pair.of(secretId, region);
    final Properties updatedProperties = new Properties();
    updatedProperties.putAll(connectionUrl.getOriginalProperties());
    Secret secret = SECRET_CACHE.get(secretKey);

    try {
      // Attempt to open initial connection with cached secret
      attemptConnectionWithSecrets(updatedProperties, secret, connectionUrl);

    } catch (SQLException connectionFailedException) {
      // Rethrow the exception unless it was because user access was denied. In that case, retry with new credentials
      if (!SQLSTATE_ACCESS_ERROR.equals(connectionFailedException.getSQLState())) {
        throw connectionFailedException;

      } else {
        try {
          secret = getCurrentCredentials();

        } catch (SecretsManagerException | JsonProcessingException getSecretsFailedException) {
          this.logger.logError(ERROR_GET_SECRETS_FAILED);
          throw new SQLException(ERROR_GET_SECRETS_FAILED);
        }

        SECRET_CACHE.put(secretKey, secret);
        attemptConnectionWithSecrets(updatedProperties, secret, connectionUrl);
      }
    }
  }

  private void attemptConnectionWithSecrets(Properties props, Secret secret, ConnectionUrl connectionUrl) throws SQLException {
    updateConnectionProperties(props, secret);
    final ConnectionUrl newConnectionUrl = ConnectionUrl.getConnectionUrlInstance(connectionUrl.getDatabaseUrl(), props);
    this.nextPlugin.openInitialConnection(newConnectionUrl);
  }

  private void updateConnectionProperties(Properties props, Secret secret) {
    if (secret != null) {
      props.put("user", secret.getUsername());
      props.put("password", secret.getPassword());
    }
  }

  Secret getCurrentCredentials() throws SecretsManagerException, JsonProcessingException {
    final GetSecretValueResponse valueResponse = this.secretsManagerClient.getSecretValue(this.getSecretValueRequest);
    final ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(valueResponse.secretString(), Secret.class);
  }

  @Override
  public Object execute(
      Class<?> methodInvokeOn,
      String methodName,
      Callable<?> executeSqlFunc,
      Object[] args)
      throws Exception {
    return this.nextPlugin.execute(methodInvokeOn, methodName, executeSqlFunc, args);
  }

  @Override
  public void transactionBegun() {
    this.nextPlugin.transactionBegun();
  }

  @Override
  public void transactionCompleted() {
    this.nextPlugin.transactionCompleted();
  }

  @Override
  public void releaseResources() {
    this.nextPlugin.releaseResources();
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  static class Secret {
    @JsonProperty("username")
    private String username;
    @JsonProperty("password")
    private String password;

    Secret() {
    }

    Secret(String username, String password) {
      this.username = username;
      this.password = password;
    }

    String getUsername() {
      return this.username;
    }

    String getPassword() {
      return this.password;
    }
  }
}
