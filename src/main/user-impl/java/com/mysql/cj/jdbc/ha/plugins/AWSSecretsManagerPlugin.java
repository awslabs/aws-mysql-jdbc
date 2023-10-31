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
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.jdbc.ha.util.ConnectionUtils;
import com.mysql.cj.log.Log;
import com.mysql.cj.util.LRUCache;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mysql.cj.util.StringUtils;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;
import software.amazon.awssdk.utils.Pair;

import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.function.Function;

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
  private final ICurrentConnectionProvider currentConnectionProvider;
  private final IConnectionPlugin nextPlugin;
  private final Log logger;
  private final String secretId;
  private final Region region;
  private final Function<Region, SecretsManagerClient> secretsManagerClientFunc;
  private final Function<String, GetSecretValueRequest> getSecretValueRequestFunc;
  private final Pair<String, Region> secretKey;
  private Secret secret;

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
        (region) -> SecretsManagerClient.builder()
            .region(region)
            .build(),
        (secretId) -> GetSecretValueRequest.builder()
            .secretId(secretId)
            .build()
    );
  }

  AWSSecretsManagerPlugin(
      ICurrentConnectionProvider currentConnectionProvider,
      PropertySet propertySet,
      IConnectionPlugin nextPlugin,
      Log logger,
      Function<Region, SecretsManagerClient> secretsManagerClientFunc,
      Function<String, GetSecretValueRequest> getSecretValueRequestFunc) throws SQLException {

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

    if (StringUtils.isNullOrEmpty(propertySet.getStringProperty(SECRET_ID_PROPERTY).getValue())) {
      throw new SQLException(
          String.format("Configuration parameter '%s' is required.", SECRET_ID_PROPERTY));
    }

    if (StringUtils.isNullOrEmpty(propertySet.getStringProperty(REGION_PROPERTY).getValue())) {
      throw new SQLException(
          String.format("Configuration parameter '%s' is required.", REGION_PROPERTY));
    }

    this.currentConnectionProvider = currentConnectionProvider;
    this.nextPlugin = nextPlugin;
    this.logger = logger;
    this.secretId = propertySet.getStringProperty(SECRET_ID_PROPERTY).getValue();
    this.region = Region.of(propertySet.getStringProperty(REGION_PROPERTY).getValue());
    this.secretKey = Pair.of(secretId, region);

    this.secretsManagerClientFunc = secretsManagerClientFunc;
    this.getSecretValueRequestFunc = getSecretValueRequestFunc;
  }

  @Override
  public void openInitialConnection(ConnectionUrl connectionUrl) throws SQLException {

    final Properties properties = new Properties();
    properties.putAll(connectionUrl.getOriginalProperties());

    boolean secretWasFetched = updateSecret(false);

    try {
      applySecretToProperties(properties);
      attemptToLogin(properties, connectionUrl);

    } catch (SQLException exception) {
      if (isLoginUnsuccessful(exception) && !secretWasFetched) {
        // Login unsuccessful with cached credentials
        // Try to re-fetch credentials and try again

        secretWasFetched = updateSecret(true);
        if (secretWasFetched) {
          applySecretToProperties(properties);
          attemptToLogin(properties, connectionUrl);
          return;
        }
      }
      throw exception;
    } catch (Exception exception) {
      this.logger.logError("Unhandled exception:", exception);
      throw new SQLException(exception);
    }
  }

  /**
   * Called to analyse a thrown exception.
   *
   * @param exception Login attempt exception.
   * @return true, if specified exception is caused by unsuccessful login attempt
   */
  private boolean isLoginUnsuccessful(SQLException exception) {
    this.logger.logTrace("Login failed. SQLState=" + exception.getSQLState(), exception);

    Throwable throwable = exception;
    while (throwable != null) {
      String sqlState = "";
      if (throwable instanceof SQLException) {
        sqlState = ((SQLException) throwable).getSQLState();
      } else if (throwable instanceof CJException) {
        sqlState = ((CJException) throwable).getSQLState();
      }

      if (SQLSTATE_ACCESS_ERROR.equals(sqlState)) {
        return true;
      }

      throwable = throwable.getCause();
    }

    return false;
  }

  /**
   * Updates credentials to provided properties.
   *
   * @param properties Properties to store credentials.
   */
  private void applySecretToProperties(Properties properties) {
    if (this.secret != null) {
      /** Updated credentials are stored in properties. Other plugins in the plugin chain may
       * change them if needed. Eventually, credentials will be used to open a new connection in
       * {@link DefaultConnectionPlugin#openInitialConnection}
       */
      properties.put("user", secret.getUsername());
      properties.put("password", secret.getPassword());
    }
  }

  /**
   * Called to update credentials from the cache, or from AWS Secret Manager service.
   *
   * @param forceReFetch Allows ignoring cached credentials and force to fetch the latest credentials from the service.
   * @return true, if credentials were fetched from the service.
   */
  private boolean updateSecret(boolean forceReFetch) throws SQLException {

    boolean fetched = false;
    this.secret = SECRET_CACHE.get(this.secretKey);

    if (secret == null || forceReFetch) {
      try {
        this.secret = fetchLatestCredentials();
        if (this.secret != null) {
          fetched = true;
          SECRET_CACHE.put(this.secretKey, this.secret);
        }
      } catch (SecretsManagerException | JsonProcessingException exception) {
        this.logger.logError(ERROR_GET_SECRETS_FAILED, exception);
        throw new SQLException(ERROR_GET_SECRETS_FAILED, exception);
      }
    }
    return fetched;
  }

  private void updateCurrentHostInfoWithSecrets(HostInfo hostInfo) {
    HostInfo currentHostInfo = this.currentConnectionProvider.getCurrentHostInfo();
    this.currentConnectionProvider.setCurrentConnection(
        this.currentConnectionProvider.getCurrentConnection(),
        ConnectionUtils.copyWithAdditionalProps(currentHostInfo, hostInfo));
  }

  /**
   * Called to open a new connection. This plugin is responsible to providing a recent credentials and delegate actual
   * opening a new connection to other plugins in the plugin chain. Eventually a new connection is handled either by
   * some plugin, or by {@link DefaultConnectionPlugin#openInitialConnection}
   *
   * @param props         Properties with updated credentials.
   * @param connectionUrl Original instance of ConnectionUrl
   */
  private void attemptToLogin(Properties props, ConnectionUrl connectionUrl)
      throws SQLException {

    final ConnectionUrl newConnectionUrl =
        ConnectionUrl.getConnectionUrlInstance(connectionUrl.getDatabaseUrl(),
            props);
    this.nextPlugin.openInitialConnection(newConnectionUrl);
    updateCurrentHostInfoWithSecrets(newConnectionUrl.getMainHost());
  }

  Secret fetchLatestCredentials() throws SecretsManagerException, JsonProcessingException {
    final SecretsManagerClient client = this.secretsManagerClientFunc.apply(this.region);
    final GetSecretValueRequest request = this.getSecretValueRequestFunc.apply(this.secretId);

    final GetSecretValueResponse valueResponse;
    try {
      valueResponse = client.getSecretValue(request);
    } finally {
      client.close();
    }

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
