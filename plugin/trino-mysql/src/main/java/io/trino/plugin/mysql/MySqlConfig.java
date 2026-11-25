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
package io.trino.plugin.mysql;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import jakarta.validation.constraints.Min;

import java.util.concurrent.TimeUnit;

import static io.trino.jsonresultimpl.JSONResultImpConstants.ADDITIONAL_DATA;
import static io.trino.jsonresultimpl.JSONResultImpConstants.AUTHENTICATION_KEY;
import static io.trino.jsonresultimpl.JSONResultImpConstants.AUTHENTICATION_RESOURCE;
import static io.trino.jsonresultimpl.JSONResultImpConstants.AUTHENTICATION_URL;
import static io.trino.jsonresultimpl.JSONResultImpConstants.CURRENT_BATCH_KEY;
import static io.trino.jsonresultimpl.JSONResultImpConstants.GET_NEXT_BATCH_URL;
import static io.trino.jsonresultimpl.JSONResultImpConstants.GET_RESULT_URL;
import static io.trino.jsonresultimpl.JSONResultImpConstants.NEXT_BATCH_KEY;

public class MySqlConfig
{
    private boolean autoReconnect = true;
    private int maxReconnects = 3;
    private Duration connectionTimeout = new Duration(10, TimeUnit.SECONDS);
    private String authenticationUrl;
    private String authenticationKey;
    private String authenticationResource;
    private String getResultUrl;
    private String getNextBatchUrl;
    private String currentBatchKey;
    private String nextBatchKey;
    private String additionalData;

    // Using `useInformationSchema=true` prevents race condition inside MySQL driver's java.sql.DatabaseMetaData.getColumns
    // implementation, which throw SQL exception when a table disappears during listing.
    // Using `useInformationSchema=false` may provide more diagnostic information (see https://github.com/trinodb/trino/issues/1597)
    private boolean driverUseInformationSchema = true;

    public boolean isAutoReconnect()
    {
        return autoReconnect;
    }

    @Config("mysql.auto-reconnect")
    public MySqlConfig setAutoReconnect(boolean autoReconnect)
    {
        this.autoReconnect = autoReconnect;
        return this;
    }

    @Min(1)
    public int getMaxReconnects()
    {
        return maxReconnects;
    }

    @Config("mysql.max-reconnects")
    public MySqlConfig setMaxReconnects(int maxReconnects)
    {
        this.maxReconnects = maxReconnects;
        return this;
    }

    public Duration getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("mysql.connection-timeout")
    public MySqlConfig setConnectionTimeout(Duration connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    public boolean isDriverUseInformationSchema()
    {
        return driverUseInformationSchema;
    }

    @Config("mysql.jdbc.use-information-schema")
    @ConfigDescription("Value of useInformationSchema MySQL JDBC driver connection property")
    public MySqlConfig setDriverUseInformationSchema(boolean driverUseInformationSchema)
    {
        this.driverUseInformationSchema = driverUseInformationSchema;
        return this;
    }

    public String getAuthenticationUrl()
    {
        return authenticationUrl;
    }

    @Config(AUTHENTICATION_URL)
    public MySqlConfig setAuthenticationUrl(String authenticationUrl)
    {
        this.authenticationUrl = authenticationUrl;
        return this;
    }

    public String getAuthenticationKey()
    {
        return authenticationKey;
    }

    @Config(AUTHENTICATION_KEY)
    public MySqlConfig setAuthenticationKey(String authenticationKey)
    {
        this.authenticationKey = authenticationKey;
        return this;
    }

    public String getAuthenticationResource()
    {
        return authenticationResource;
    }

    @Config(AUTHENTICATION_RESOURCE)
    public MySqlConfig setAuthenticationResource(String authenticationResource)
    {
        this.authenticationResource = authenticationResource;
        return this;
    }

    public String getGetResultUrl()
    {
        return getResultUrl;
    }

    @Config(GET_RESULT_URL)
    public MySqlConfig setGetResultUrl(String getResultUrl)
    {
        this.getResultUrl = getResultUrl;
        return this;
    }

    public String getGetNextBatchUrl()
    {
        return getNextBatchUrl;
    }

    @Config(GET_NEXT_BATCH_URL)
    public MySqlConfig setGetNextBatchUrl(String getNextBatchUrl)
    {
        this.getNextBatchUrl = getNextBatchUrl;
        return this;
    }

    public String getAdditionalData()
    {
        return additionalData;
    }

    @Config(ADDITIONAL_DATA)
    public MySqlConfig setAdditionalData(String additionalData)
    {
        this.additionalData = additionalData;
        return this;
    }

    public String getCurrentBatchKey()
    {
        return currentBatchKey;
    }

    @Config(CURRENT_BATCH_KEY)
    public MySqlConfig setCurrentBatchKey(String currentBatchKey)
    {
        this.currentBatchKey = currentBatchKey;
        return this;
    }

    public String getNextBatchKey()
    {
        return nextBatchKey;
    }

    @Config(NEXT_BATCH_KEY)
    public MySqlConfig setNextBatchKey(String nextBatchKey)
    {
        this.nextBatchKey = nextBatchKey;
        return this;
    }
}
