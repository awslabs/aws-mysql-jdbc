/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0
 * (GPLv2), as published by the Free Software Foundation, with the
 * following additional permissions:
 *
 * This program is distributed with certain software that is licensed
 * under separate terms, as designated in a particular file or component
 * or in the license documentation. Without limiting your rights under
 * the GPLv2, the authors of this program hereby grant you an additional
 * permission to link the program and your derivative works with the
 * separately licensed software that they have included with the program.
 *
 * Without limiting the foregoing grant of rights under the GPLv2 and
 * additional permission as to separately licensed software, this
 * program is also subject to the Universal FOSS Exception, version 1.0,
 * a copy of which can be found along with its FAQ at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see
 * http://www.gnu.org/licenses/gpl-2.0.html.
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
  private static final String ERROR_MISSING_DEPENDENCY_SECRETS =
      "[AWSSecretsManagerPlugin] Required dependency 'AWS Java SDK for AWS Secrets Manager' is not on the classpath";
  private static final String ERROR_MISSING_DEPENDENCY_JACKSON =
      "[AWSSecretsManagerPlugin] Required dependency 'Jackson Databind' is not on the classpath";
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
      logger.logError(ERROR_MISSING_DEPENDENCY_SECRETS);
      throw new SQLException(ERROR_MISSING_DEPENDENCY_SECRETS);
    }

    try {
      Class.forName("com.fasterxml.jackson.databind.ObjectMapper");
    } catch (ClassNotFoundException e) {
      logger.logError(ERROR_MISSING_DEPENDENCY_JACKSON);
      throw new SQLException(ERROR_MISSING_DEPENDENCY_JACKSON);
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
    final Properties properties = new Properties();
    properties.putAll(connectionUrl.getOriginalProperties());
    Secret secret = SECRET_CACHE.get(secretKey);

      // Attempt to open initial connection with cached secret.
      if (secret != null) {
        attemptConnectionWithSecrets(properties, secret, connectionUrl);
      } else {
        try {
          secret = getCurrentCredentials();
        } catch (SecretsManagerException | JsonProcessingException getSecretsFailedException) {
          this.logger.logError(ERROR_GET_SECRETS_FAILED);
          throw new SQLException(ERROR_GET_SECRETS_FAILED);
        }
        if (secret != null) {
          SECRET_CACHE.put(secretKey, secret);
          attemptConnectionWithSecrets(properties, secret, connectionUrl);
        } else {
          this.logger.logError(ERROR_GET_SECRETS_FAILED);
          throw new SQLException(ERROR_GET_SECRETS_FAILED);
        }
      }
  }

  private void attemptConnectionWithSecrets(Properties props, Secret secret, ConnectionUrl connectionUrl) throws SQLException {
    if (secret != null) {
      props.put("user", secret.getUsername());
      props.put("password", secret.getPassword());
    }

    final ConnectionUrl newConnectionUrl = ConnectionUrl.getConnectionUrlInstance(connectionUrl.getDatabaseUrl(), props);
    this.nextPlugin.openInitialConnection(newConnectionUrl);
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
