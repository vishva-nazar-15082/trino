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

package io.trino.jsonresultimpl;

import org.json.JSONObject;

import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Executor;

import static io.trino.jsonresultimpl.JSONResultImpConstants.ADDITIONAL_DATA;
import static io.trino.jsonresultimpl.JSONResultImpConstants.AUTHENTICATION_KEY;
import static io.trino.jsonresultimpl.JSONResultImpConstants.AUTHENTICATION_RESOURCE;
import static io.trino.jsonresultimpl.JSONResultImpConstants.AUTHENTICATION_URL;
import static io.trino.jsonresultimpl.JSONResultImpConstants.CURRENT_BATCH_KEY;
import static io.trino.jsonresultimpl.JSONResultImpConstants.GET_NEXT_BATCH_URL;
import static io.trino.jsonresultimpl.JSONResultImpConstants.GET_RESULT_URL;
import static io.trino.jsonresultimpl.JSONResultImpConstants.NEXT_BATCH_KEY;
import static io.trino.jsonresultimpl.JSONResultImpConstants.RESULT;
import static io.trino.jsonresultimpl.JSONResultImpConstants.RESULT_TYPE;
import static io.trino.jsonresultimpl.JSONResultImpConstants.SQL_QUERY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

public class JSONRSRestfulConnection
        implements Connection, JSONResultBatchFetcher
{

    private final JSONObject props;

    public JSONRSRestfulConnection(JSONObject props)
    {
        this.props = props;
        authenticate();
    }

    public HttpResponse<String> getResponse(String url, JSONResultImpConstants.RequestMethod requestMethod, String requestBody, String... headers)
    {
        HttpRequest.Builder reqBuilder = HttpRequest.newBuilder().uri(URI.create(url));
        HttpRequest.BodyPublisher body = requestBody != null ? HttpRequest.BodyPublishers.ofString(requestBody) : HttpRequest.BodyPublishers.noBody();
        reqBuilder.method(requestMethod.name(), body);
        reqBuilder.headers(headers);
        HttpClient client = null;
        try {
            client = HttpClient.newBuilder().authenticator(Authenticator.getDefault()).build();
            return client.send(reqBuilder.build(), HttpResponse.BodyHandlers.ofString());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            if (Objects.nonNull(client) && client instanceof AutoCloseable) {
                try {
                    client.getClass().getMethod("close").invoke(client);
                }
                catch (Exception ignored) {

                }
            }
        }
    }

    public void authenticate()
    {
        String[] headers = new String[] {"Content-Type", "text/plain", AUTHENTICATION_RESOURCE, props.getString(AUTHENTICATION_RESOURCE), AUTHENTICATION_KEY,
                props.getString(AUTHENTICATION_KEY)};
        HttpResponse<String> response = getResponse(props.getString(AUTHENTICATION_URL), JSONResultImpConstants.RequestMethod.POST, null, headers);
        if (response.statusCode() == HttpURLConnection.HTTP_OK) {
            JSONObject httpResp = new JSONObject(response.body());
            props.put(AUTHENTICATION_KEY, httpResp.optString(AUTHENTICATION_KEY, props.getString(AUTHENTICATION_KEY)));
        }
    }

    public JSONResultSet getResult(String resultType, String sql)
    {
        String[] headers = new String[] {"Content-Type", "text/plain", AUTHENTICATION_RESOURCE, props.getString(AUTHENTICATION_RESOURCE), AUTHENTICATION_KEY,
                props.getString(AUTHENTICATION_KEY)};
        JSONObject requestBody = new JSONObject();
        requestBody.put(RESULT_TYPE, resultType);
        if(sql != null){
            requestBody.put(SQL_QUERY, URLEncoder.encode(sql, StandardCharsets.UTF_8));
        }
        requestBody.put(ADDITIONAL_DATA, props.getString(ADDITIONAL_DATA));
        HttpResponse<String> response = getResponse(props.getString(GET_RESULT_URL), JSONResultImpConstants.RequestMethod.POST, requestBody.toString(), headers);
        JSONObject respJson = new JSONObject(response.body());
        props.put(CURRENT_BATCH_KEY, respJson.optString(CURRENT_BATCH_KEY, "-1"));
        props.put(NEXT_BATCH_KEY, respJson.optString(NEXT_BATCH_KEY, "-1"));
        return new JSONResultSet(respJson.get(RESULT).toString(), this);
    }

    public JSONObject getNextBatch()
    {
        try{
            String[] headers = new String[] {"Content-Type", "text/plain", AUTHENTICATION_RESOURCE, props.getString(AUTHENTICATION_RESOURCE), AUTHENTICATION_KEY,
                    props.getString(AUTHENTICATION_KEY), CURRENT_BATCH_KEY, props.optString(CURRENT_BATCH_KEY, "-1"), NEXT_BATCH_KEY, props.optString(NEXT_BATCH_KEY, "-1")};
            JSONObject requestBody = new JSONObject();
            HttpResponse<String> response = getResponse(props.getString(GET_NEXT_BATCH_URL), JSONResultImpConstants.RequestMethod.POST, requestBody.toString(), headers);
            JSONObject respJson = new JSONObject(response.body());
            props.put(CURRENT_BATCH_KEY, respJson.optString(CURRENT_BATCH_KEY, "-1"));
            props.put(NEXT_BATCH_KEY, respJson.optString(NEXT_BATCH_KEY, "-1"));
            return respJson.getJSONObject(RESULT);
        }catch (Exception e){
            return null;
        }
    }

    @Override
    public Statement createStatement()
            throws SQLException
    {
        return new JSONRSStatement(this, TYPE_FORWARD_ONLY);
    }

    @Override
    public PreparedStatement prepareStatement(String sql)
            throws SQLException
    {
        return new JSONRSStatement(this, TYPE_FORWARD_ONLY, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql)
            throws SQLException
    {
        return null;
    }

    @Override
    public String nativeSQL(String sql)
            throws SQLException
    {
        return "";
    }

    @Override
    public void setAutoCommit(boolean autoCommit)
            throws SQLException
    {
    }

    @Override
    public boolean getAutoCommit()
            throws SQLException
    {
        return false;
    }

    @Override
    public void commit()
            throws SQLException
    {
    }

    @Override
    public void rollback()
            throws SQLException
    {
    }

    @Override
    public void close()
            throws SQLException
    {
    }

    @Override
    public boolean isClosed()
            throws SQLException
    {
        return false;
    }

    @Override
    public DatabaseMetaData getMetaData()
            throws SQLException
    {
        return new JSONRestfulConnectionMetaData(this);
    }

    @Override
    public void setReadOnly(boolean readOnly)
            throws SQLException
    {
    }

    @Override
    public boolean isReadOnly()
            throws SQLException
    {
        return false;
    }

    @Override
    public void setCatalog(String catalog)
            throws SQLException
    {
    }

    @Override
    public String getCatalog()
            throws SQLException
    {
        return "";
    }

    @Override
    public void setTransactionIsolation(int level)
            throws SQLException
    {
    }

    @Override
    public int getTransactionIsolation()
            throws SQLException
    {
        return 0;
    }

    @Override
    public SQLWarning getWarnings()
            throws SQLException
    {
        return null;
    }

    @Override
    public void clearWarnings()
            throws SQLException
    {
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException
    {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException
    {
        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException
    {
        return null;
    }

    @Override
    public Map<String, Class<?>> getTypeMap()
            throws SQLException
    {
        return Map.of();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map)
            throws SQLException
    {
    }

    @Override
    public void setHoldability(int holdability)
            throws SQLException
    {
    }

    @Override
    public int getHoldability()
            throws SQLException
    {
        return 0;
    }

    @Override
    public Savepoint setSavepoint()
            throws SQLException
    {
        return null;
    }

    @Override
    public Savepoint setSavepoint(String name)
            throws SQLException
    {
        return null;
    }

    @Override
    public void rollback(Savepoint savepoint)
            throws SQLException
    {
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint)
            throws SQLException
    {
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException
    {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException
    {
        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException
    {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws SQLException
    {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
            throws SQLException
    {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames)
            throws SQLException
    {
        return null;
    }

    @Override
    public Clob createClob()
            throws SQLException
    {
        return null;
    }

    @Override
    public Blob createBlob()
            throws SQLException
    {
        return null;
    }

    @Override
    public NClob createNClob()
            throws SQLException
    {
        return null;
    }

    @Override
    public SQLXML createSQLXML()
            throws SQLException
    {
        return null;
    }

    @Override
    public boolean isValid(int timeout)
            throws SQLException
    {
        return false;
    }

    @Override
    public void setClientInfo(String name, String value)
            throws SQLClientInfoException
    {
    }

    @Override
    public void setClientInfo(Properties properties)
            throws SQLClientInfoException
    {
    }

    @Override
    public String getClientInfo(String name)
            throws SQLException
    {
        return "";
    }

    @Override
    public Properties getClientInfo()
            throws SQLException
    {
        return null;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements)
            throws SQLException
    {
        return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes)
            throws SQLException
    {
        return null;
    }

    @Override
    public void setSchema(String schema)
            throws SQLException
    {
    }

    @Override
    public String getSchema()
            throws SQLException
    {
        return "";
    }

    @Override
    public void abort(Executor executor)
            throws SQLException
    {
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds)
            throws SQLException
    {
    }

    @Override
    public int getNetworkTimeout()
            throws SQLException
    {
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> iface)
            throws SQLException
    {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface)
            throws SQLException
    {
        return false;
    }
}
