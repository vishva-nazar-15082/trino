/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.jdbc;

import io.airlift.log.Logger;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.connectivity.SSHTunnelUtil;
import io.trino.connectivity.SSHTunnelUtil.SSHTunnel;
import io.trino.connectivity.SSHTunnelUtil.SSHTunnelCredential;
import io.trino.plugin.jdbc.credential.CredentialPropertiesProvider;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.DefaultCredentialPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DriverConnectionFactory
        implements ConnectionFactory
{
    private static final Logger log = Logger.get(DriverConnectionFactory.class);
    private static final String HOST_PLACEHOLDER = "__JDBC_HOST__";
    private static final String PORT_PLACEHOLDER = "__JDBC_PORT__";

    private final Driver driver;
    private final AtomicReference<String> connectionUrl;
    private final String originalConnectionUrl;
    private final List<String> connectionParts = new ArrayList<>();
    private final String connectionHost;
    private final int connectionPort;
    private final Properties connectionProperties;
    private final CredentialPropertiesProvider credentialPropertiesProvider;
    private final TracingDataSource dataSource;
    private volatile SSHTunnel tunnel;
    private final SSHTunnelCredential tunnelCredential;

    public DriverConnectionFactory(
            Driver driver,
            String connectionUrl,
            Properties connectionProperties,
            String connectionHost,
            int connectionPort,
            CredentialPropertiesProvider credentialPropertiesProvider,
            OpenTelemetry openTelemetry,
            SSHTunnelCredential tunnelCredential)
    {
        this.driver = driver;
        this.originalConnectionUrl = requireNonNull(connectionUrl, "connectionUrl is null");
        this.connectionHost = connectionHost;
        this.connectionPort = connectionPort;
        this.connectionUrl = new AtomicReference<>(originalConnectionUrl);
        this.connectionProperties = new Properties();
        this.connectionProperties.putAll(requireNonNull(connectionProperties, "connectionProperties is null"));
        this.credentialPropertiesProvider = credentialPropertiesProvider;
        this.tunnelCredential = tunnelCredential;
        if (driver == null) {
            this.dataSource = null;
        }
        else {
            requireNonNull(credentialPropertiesProvider, "credentialPropertiesProvider is null");
            this.dataSource = new TracingDataSource(requireNonNull(openTelemetry, "openTelemetry is null"), driver, this.connectionUrl);
        }
        setUpTunnelIfRequired();
    }

    public String currentConnectionUrl()
    {
        return connectionUrl.get();
    }

    public void setUpTunnelIfRequired()
    {
        int hostIndex = originalConnectionUrl.indexOf(HOST_PLACEHOLDER);
        int portIndex = originalConnectionUrl.indexOf(PORT_PLACEHOLDER);
        if (hostIndex == -1 || portIndex == -1 || hostIndex > portIndex) {
            return;
        }
        connectionParts.add(originalConnectionUrl.substring(0, hostIndex));
        connectionParts.add(HOST_PLACEHOLDER);
        connectionParts.add(originalConnectionUrl.substring(hostIndex + HOST_PLACEHOLDER.length(), portIndex));
        connectionParts.add(PORT_PLACEHOLDER);
        connectionParts.add(originalConnectionUrl.substring(portIndex + PORT_PLACEHOLDER.length()));
        try {
            this.tunnel = SSHTunnelUtil.getTunnel(connectionHost, connectionPort, credentialOrLocal());
            updateTunnelAddress();
        }
        catch (Exception e) {
            log.error(e, "Exception in tunnel creation");
        }
    }

    public synchronized void checkTunnel()
    {
        if (tunnel == null) {
            return;
        }
        if (tunnel.isClosed() || tunnel.isExpired()) {
            try {
                tunnel = SSHTunnelUtil.getTunnel(connectionHost, connectionPort, credentialOrLocal());
                updateTunnelAddress();
            }
            catch (Exception e) {
                log.error(e, "Exception in tunnel check");
            }
        }
    }

    public synchronized void updateTunnelAddress()
    {
        if (tunnel == null || connectionParts.size() < 4) {
            return;
        }
        try {
            connectionParts.set(1, tunnel.getBoundHost());
            connectionParts.set(3, String.valueOf(tunnel.getBoundPort()));
            connectionUrl.set(String.join("", connectionParts));
        }
        catch (RuntimeException e) {
            log.error(e, "Exception in updating tunnel address");
        }
    }

    protected void lockTunnel(int retries)
    {
        if (tunnel == null) {
            return;
        }
        if (retries > 1) {
            throw new RuntimeException("Failed to lock tunnel after 2 retries");
        }
        try {
            tunnel.lock().readLock().lock();
        }
        catch (IllegalStateException e) {
            if (tunnel.isClosed() || tunnel.isExpired()) {
                checkTunnel();
                lockTunnel(retries + 1);
            }
        }
        catch (RuntimeException e) {
            log.error(e, "Exception in locking tunnel");
        }
    }

    protected Connection finishConnection(Connection connection)
    {
        lockTunnel(0);
        return new TunneledConnection(connection, tunnel);
    }

    private SSHTunnelCredential credentialOrLocal()
    {
        if (tunnelCredential != null && tunnelCredential.isValid()) {
            return tunnelCredential;
        }
        return SSHTunnelUtil.LOCAL_TUNNEL_CREDENTIAL;
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        if (tunnel != null) {
            checkTunnel();
            updateTunnelAddress();
        }
        else {
            connectionUrl.set(originalConnectionUrl);
        }
        Properties properties = getCredentialProperties(session.getIdentity());
        Connection connection = requireNonNull(dataSource, "dataSource is null").getConnection(properties);
        checkState(connection != null, "Driver returned null connection, make sure the connection URL '%s' is valid for the driver %s", connectionUrl, driver);
        return finishConnection(connection);
    }

    private Properties getCredentialProperties(ConnectorIdentity identity)
    {
        Properties properties = new Properties();
        properties.putAll(connectionProperties);
        properties.putAll(credentialPropertiesProvider.getCredentialProperties(identity));
        return properties;
    }

    public static Builder builder(Driver driver, String connectionUrl, CredentialProvider credentialProvider)
    {
        return new Builder(driver, connectionUrl, null, 0, credentialProvider, null);
    }

    public static Builder builder(
            Driver driver,
            String connectionUrl,
            String connectionHost,
            int connectionPort,
            CredentialProvider credentialProvider,
            SSHTunnelCredential tunnelCredential)
    {
        return new Builder(driver, connectionUrl, connectionHost, connectionPort, credentialProvider, tunnelCredential);
    }

    public static class Builder
    {
        private final Driver driver;
        private final String connectionUrl;
        private final String connectionHost;
        private final int connectionPort;
        private final SSHTunnelCredential tunnelCredential;
        private Properties connectionProperties = new Properties();
        private CredentialPropertiesProvider credentialPropertiesProvider;
        private OpenTelemetry openTelemetry = OpenTelemetry.noop();

        private Builder(
                Driver driver,
                String connectionUrl,
                String connectionHost,
                int connectionPort,
                CredentialProvider credentialProvider,
                SSHTunnelCredential tunnelCredential)
        {
            this.driver = requireNonNull(driver, "driver is null");
            this.connectionUrl = requireNonNull(connectionUrl, "connectionUrl is null");
            this.connectionHost = connectionHost;
            this.connectionPort = connectionPort;
            this.tunnelCredential = tunnelCredential;
            this.credentialPropertiesProvider = new DefaultCredentialPropertiesProvider(requireNonNull(credentialProvider, "credentialProvider is null"));
        }

        public Builder setConnectionProperties(Properties connectionProperties)
        {
            this.connectionProperties = connectionProperties;
            return this;
        }

        public Builder setCredentialPropertiesProvider(CredentialPropertiesProvider credentialPropertiesProvider)
        {
            this.credentialPropertiesProvider = credentialPropertiesProvider;
            return this;
        }

        public Builder setOpenTelemetry(OpenTelemetry openTelemetry)
        {
            this.openTelemetry = openTelemetry;
            return this;
        }

        public DriverConnectionFactory build()
        {
            return new DriverConnectionFactory(
                    driver,
                    connectionUrl,
                    connectionProperties,
                    connectionHost,
                    connectionPort,
                    credentialPropertiesProvider,
                    openTelemetry,
                    tunnelCredential);
        }
    }
}
