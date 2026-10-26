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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.Serializable;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import static io.trino.jsonresultimpl.JSONResultImpConstants.DEFAULT_PREFETCH_THRESHOLD_PERCENT;
import static io.trino.jsonresultimpl.JSONResultImpConstants.RESULT_DATA;
import static io.trino.jsonresultimpl.JSONResultImpConstants.RESULT_ID_NAME_MAPPING;
import static io.trino.jsonresultimpl.JSONResultImpConstants.RESULT_ID_TYPE_MAPPING;
import static io.trino.jsonresultimpl.JSONResultImpConstants.RESULT_IS_JAVA_COL_TYPE;
import static io.trino.jsonresultimpl.JSONResultImpConstants.NULL_MARKER;

/**
 * @author vishva-15082
 */
public class JSONResultSet
        implements ResultSet, Serializable
{

    private JSONArray jsonArray;

    private ArrayList<JSONArray> resultBatches = new ArrayList<>();

    private ListIterator<JSONArray> resultBatchItr = resultBatches.listIterator();

    private final JSONObject colIdNameMapping;

    private final JSONObject colIdTypeMapping;

    private final boolean isJavaColType;

    private int curRowInd = -1;

    private int rowCount;

    private int prefetchAtRow = Integer.MAX_VALUE;

    private JSONArray currentBatch;

    private JSONObject currentRow;

    private JSONResultSetMetaData rsmd;

    private int resultSetType = TYPE_FORWARD_ONLY;

    private static final int PREFETCH_THRESHOLD_PERCENT = Integer.parseInt(DEFAULT_PREFETCH_THRESHOLD_PERCENT);

    private static final ExecutorService PREFETCH_EXECUTOR = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable, "json-result-prefetch");
        thread.setDaemon(true);
        return thread;
    });

    private final JSONResultBatchFetcher batchFetcher;

    private transient Future<JSONObject> prefetchedBatch;

    private transient boolean prefetchStarted;

    private boolean wasLastReturnNull;

    public JSONResultSet(String result)
    {
        this(result, (JSONResultBatchFetcher) null);
    }

    public JSONResultSet(String result, JSONRSRestfulConnection connection)
    {
        this(result, (JSONResultBatchFetcher) connection);
    }

    public JSONResultSet(String result, JSONResultBatchFetcher batchFetcher)
    {
        JSONObject response = new JSONObject(result);
        Object resultData = response.get(RESULT_DATA);
        this.resultBatches.add(new JSONArray(resultData.toString()));
        this.resultBatchItr = resultBatches.listIterator(0);
        this.colIdNameMapping = response.optJSONObject(RESULT_ID_NAME_MAPPING);
        this.colIdTypeMapping = response.optJSONObject(RESULT_ID_TYPE_MAPPING);
        this.isJavaColType = response.optBoolean(RESULT_IS_JAVA_COL_TYPE);
        currentBatch = resultBatchItr.next();
        activateBatch(currentBatch);
        this.batchFetcher = batchFetcher;
    }

    public void appendNextBatch(JSONObject nextBatchResult)
    {
        if (nextBatchResult != null && nextBatchResult.has(RESULT_DATA)) {
            this.resultBatches.add(new JSONArray(nextBatchResult.get(RESULT_DATA).toString()));
            this.resultBatchItr = this.resultBatches.listIterator(resultBatches.size() - 1);
        }
    }

    //-----------------------------IMPLEMENTED--------------------------------------
    @Override
    public synchronized boolean next()
            throws SQLException
    {
        curRowInd++;
        boolean res = curRowInd != rowCount;
        if (res) {
            startPrefetchIfThresholdReached();
        }
        if (!res && batchFetcher != null) {
            appendNextBatch(takePrefetchedBatch());
            if (resultBatchItr.hasNext()) {
                int consumedBatchIndex = resultBatchItr.nextIndex() - 1;
                currentBatch = resultBatchItr.next();
                curRowInd = 0;
                activateBatch(currentBatch);
                if (getType() == TYPE_FORWARD_ONLY) {
                    discardBatchLater(consumedBatchIndex);
                }
                res = curRowInd != rowCount;
                if (res) {
                    startPrefetchIfThresholdReached();
                }
            }
        }
        if (res) {
            try {
                currentRow = currentBatch.getJSONObject(curRowInd);
            }
            catch (JSONException e) {
                throw new SQLException(e.getMessage());
            }
        }
        return res;
    }

    @Override
    public boolean previous()
            throws SQLException
    {
        curRowInd--;
        boolean res = curRowInd != -1;
        if (!res && resultBatchItr.hasPrevious()) {
            currentBatch = resultBatchItr.previous();
            rowCount = currentBatch.length() - 1;
            curRowInd = rowCount;
            res = curRowInd != -1;
        }
        if (res) {
            try {
                currentRow = currentBatch.getJSONObject(curRowInd);
            }
            catch (JSONException e) {
                throw new SQLException(e.getMessage());
            }
        }
        return res;
    }

    @Override
    public int getType()
            throws SQLException
    {
        return resultSetType;
    }

    public void setType(int resultSetType)
    {
        this.resultSetType = resultSetType;
    }

    @Override
    public String getString(int columnIndex)
            throws SQLException
    {
        try {
            if (isNull(columnIndex)) {
                wasLastReturnNull = true;
                return null;
            }
            String retVal = currentRow.getString(colIdNameMapping.getString(String.valueOf(columnIndex)));
            wasLastReturnNull = false;
            return retVal;
        }
        catch (JSONException e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public void close()
            throws SQLException
    {
        jsonArray = null;
        if (prefetchedBatch != null) {
            prefetchedBatch.cancel(true);
            prefetchedBatch = null;
        }
        prefetchStarted = false;
        if (batchFetcher != null) {
            batchFetcher.closeStream();
        }
    }

    private void activateBatch(JSONArray batch)
    {
        rowCount = batch.length();
        prefetchAtRow = prefetchRow(rowCount);
    }

    private static int prefetchRow(int batchRowCount)
    {
        if (batchRowCount <= 0) {
            return Integer.MAX_VALUE;
        }
        int row = (batchRowCount * PREFETCH_THRESHOLD_PERCENT + 99) / 100;
        return row <= 0 ? 1 : row;
    }

    private void startPrefetchIfThresholdReached()
    {
        if (batchFetcher == null || prefetchStarted || curRowInd + 1 < prefetchAtRow) {
            return;
        }
        prefetchStarted = true;
        prefetchedBatch = PREFETCH_EXECUTOR.submit(batchFetcher::getNextBatch);
    }

    private void discardBatchLater(int batchIndex)
    {
        if (batchIndex < 0) {
            return;
        }
        PREFETCH_EXECUTOR.execute(() -> {
            synchronized (JSONResultSet.this) {
                if (batchIndex < resultBatches.size()) {
                    resultBatches.set(batchIndex, null);
                }
            }
        });
    }

    private JSONObject takePrefetchedBatch()
            throws SQLException
    {
        prefetchStarted = false;
        Future<JSONObject> pending = prefetchedBatch;
        prefetchedBatch = null;
        if (pending == null) {
            pending = PREFETCH_EXECUTOR.submit(batchFetcher::getNextBatch);
        }
        try {
            return pending.get();
        }
        catch (CancellationException e) {
            return null;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException(e);
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof SQLException) {
                throw (SQLException) cause;
            }
            throw new SQLException(cause);
        }
    }

    @Override
    public String getString(String columnLabel)
            throws SQLException
    {
        try {
            Object value = getObject(columnLabel);
            return value != null ? value.toString() : null;
        }
        catch (JSONException e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public ResultSetMetaData getMetaData()
            throws SQLException
    {
        if (rsmd == null) {
            rsmd = new JSONResultSetMetaData(colIdNameMapping, colIdTypeMapping, isJavaColType);
        }
        return rsmd;
    }

    @Override
    public Object getObject(int columnIndex)
            throws SQLException
    {
        try {
            int colType = getMetaData().getColumnType(columnIndex);
            if (colType == Types.INTEGER) {
                try {
                    return getInt(columnIndex);
                }
                catch (ArithmeticException arEx) {
                    return getLong(columnIndex);
                }
            }
            else if (colType == Types.DECIMAL) {
                return getBigDecimal(columnIndex);
            }
            else if (colType == Types.DOUBLE || colType == Types.FLOAT) {
                return getDouble(columnIndex);
            }
            else if (colType == Types.REAL) {
                return getFloat(columnIndex);
            }
            else if (colType == Types.BIGINT) {
                return getLong(columnIndex);
            }
            else if (colType == Types.VARCHAR) {
                return getString(columnIndex);
            }
            else if (colType == Types.DATE) {
                return getDate(columnIndex);
            }
            else if (colType == Types.TIME) {
                return getTime(columnIndex);
            }
            else if (colType == Types.TIMESTAMP) {
                return getTimestamp(columnIndex);
            }
            return getObject(colIdNameMapping.getString(String.valueOf(columnIndex)));
        }
        catch (JSONException e) {
            try {
                return getObjectForEmptyValue(colIdNameMapping.getString(String.valueOf(columnIndex)));
            }
            catch (Exception ex) {
                throw new SQLException(e.getMessage());
            }
        }
    }

    @Override
    public Object getObject(String columnLabel)
            throws SQLException
    {
        try {
            Object objValue = currentRow.get(columnLabel);
            Object retVal = NULL_MARKER.equals(objValue) ? null : objValue;
            wasLastReturnNull = retVal == null;
            return retVal;
        }
        catch (JSONException e) {
            throw new SQLException(e.getMessage());
        }
    }

    public Object getObjectForEmptyValue(String columnLabel)
            throws SQLException
    {
        try {
            Object objValue = currentRow.get(columnLabel);
            Object retVal = (NULL_MARKER.equals(objValue) || objValue.toString().isEmpty()) ? null : objValue;
            wasLastReturnNull = retVal == null;
            return retVal;
        }
        catch (JSONException e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public int getInt(int columnIndex)
            throws SQLException
    {
        return getInt(getMetaData().getColumnName(columnIndex));
    }

    @Override
    public int getInt(String columnLabel)
            throws SQLException
    {
        long longVal = getLong(columnLabel);
        return Math.toIntExact(longVal);
    }

    @Override
    public long getLong(int columnIndex)
            throws SQLException
    {
        return getLong(getMetaData().getColumnName(columnIndex));
    }

    @Override
    public long getLong(String columnLabel)
            throws SQLException
    {
        boolean isNull = isNull(columnLabel);
        wasLastReturnNull = isNull;
        return isNull ? 0 : currentRow.getLong(columnLabel);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex)
            throws SQLException
    {
        return getBigDecimal(getMetaData().getColumnName(columnIndex));
    }

    @Override
    public InputStream getBinaryStream(int columnIndex)
            throws SQLException
    {
        InputStream retVal = isNull(columnIndex) ? null : new ByteArrayInputStream(currentRow.getString(getMetaData().getColumnName(columnIndex)).getBytes());
        wasLastReturnNull = retVal == null;
        return retVal;
    }

    @Override
    public Date getDate(int columnIndex)
            throws SQLException
    {
        return getDate(getMetaData().getColumnName(columnIndex));
    }

    @Override
    public Time getTime(int columnIndex)
            throws SQLException
    {
        return getTime(getMetaData().getColumnName(columnIndex));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex)
            throws SQLException
    {
        return getTimestamp(getMetaData().getColumnName(columnIndex));
    }

    public boolean isNull(int columnIndex)
            throws SQLException
    {
        return isNull(colIdNameMapping.getString(String.valueOf(columnIndex)));
    }

    public boolean isNull(String columnName)
            throws SQLException
    {
        return getObject(columnName) == null;
    }

//---------------------------------------------------------------------------

    @Override
    public boolean wasNull()
            throws SQLException
    {
        return wasLastReturnNull;
    }

    @Override
    public boolean getBoolean(int columnIndex)
            throws SQLException
    {
        return false;
    }

    @Override
    public byte getByte(int columnIndex)
            throws SQLException
    {
        return 0;
    }

    @Override
    public short getShort(int columnIndex)
            throws SQLException
    {
        return 0;
    }

    @Override
    public float getFloat(int columnIndex)
            throws SQLException
    {
        return getFloat(getMetaData().getColumnName(columnIndex));
    }

    @Override
    public double getDouble(int columnIndex)
            throws SQLException
    {
        return getDouble(getMetaData().getColumnName(columnIndex));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale)
            throws SQLException
    {
        return null;
    }

    @Override
    public byte[] getBytes(int columnIndex)
            throws SQLException
    {
        return new byte[0];
    }

    @Override
    public InputStream getAsciiStream(int columnIndex)
            throws SQLException
    {
        return null;
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex)
            throws SQLException
    {
        return null;
    }

    @Override
    public boolean getBoolean(String columnLabel)
            throws SQLException
    {
        return false;
    }

    @Override
    public byte getByte(String columnLabel)
            throws SQLException
    {
        return 0;
    }

    @Override
    public short getShort(String columnLabel)
            throws SQLException
    {
        return 0;
    }

    @Override
    public float getFloat(String columnLabel)
            throws SQLException
    {
        return (float) getDoubleValue(columnLabel);
    }

    @Override
    public double getDouble(String columnLabel)
            throws SQLException
    {
        return getDoubleValue(columnLabel);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale)
            throws SQLException
    {
        return null;
    }

    @Override
    public byte[] getBytes(String columnLabel)
            throws SQLException
    {
        return new byte[0];
    }

    @Override
    public Date getDate(String columnLabel)
            throws SQLException
    {
        Object value = getObject(columnLabel);
        if (Objects.isNull(value)) {
            wasLastReturnNull = true;
            return null;
        }
        wasLastReturnNull = false;

        if (value instanceof Date) {
            return (Date) value;
        }
        if (value instanceof java.util.Date) {
            return new Date(((java.util.Date) value).getTime());
        }
        if (value instanceof Number) {
            return new Date(((Number) value).longValue());
        }

        Timestamp timestampValue = parseTimestampString(columnLabel, value.toString());
        if (timestampValue == null) {
            wasLastReturnNull = true;
            return null;
        }
        return new Date(timestampValue.getTime());
    }

    @Override
    public Time getTime(String columnLabel)
            throws SQLException
    {
        Object value = getObject(columnLabel);
        if (Objects.isNull(value)) {
            wasLastReturnNull = true;
            return null;
        }
        wasLastReturnNull = false;

        if (value instanceof Time) {
            return (Time) value;
        }
        if (value instanceof java.util.Date) {
            return new Time(((java.util.Date) value).getTime());
        }
        if (value instanceof Number) {
            return new Time(((Number) value).longValue());
        }

        String stringValue = value.toString().trim();
        if (stringValue.isEmpty() || NULL_MARKER.equals(stringValue)) {
            wasLastReturnNull = true;
            return null;
        }

        try {
            return Time.valueOf(stringValue);
        }
        catch (IllegalArgumentException ignored) {
        }

        String normalizedIsoValue = normalizeDateTimeSeparator(stringValue);
        try {
            return Time.valueOf(LocalTime.parse(normalizedIsoValue).toString());
        }
        catch (DateTimeParseException ignored) {
        }
        try {
            return Time.valueOf(OffsetTime.parse(normalizedIsoValue).toLocalTime().toString());
        }
        catch (DateTimeParseException ignored) {
        }
        try {
            return Time.valueOf(LocalDateTime.parse(normalizedIsoValue).toLocalTime().toString());
        }
        catch (DateTimeParseException ignored) {
        }
        try {
            return Time.valueOf(OffsetDateTime.parse(normalizedIsoValue).toLocalTime().toString());
        }
        catch (DateTimeParseException exception) {
            throw new SQLException("Unable to convert column '" + columnLabel + "' value '" + value + "' to Time", exception);
        }
    }

    @Override
    public Timestamp getTimestamp(String columnLabel)
            throws SQLException
    {
        Object value = getObject(columnLabel);
        if (Objects.isNull(value)) {
            wasLastReturnNull = true;
            return null;
        }
        wasLastReturnNull = false;

        if (value instanceof Timestamp) {
            return (Timestamp) value;
        }
        if (value instanceof java.util.Date) {
            return new Timestamp(((java.util.Date) value).getTime());
        }
        if (value instanceof Number) {
            return new Timestamp(((Number) value).longValue());
        }

        return parseTimestampString(columnLabel, value.toString());
    }

    @Override
    public InputStream getAsciiStream(String columnLabel)
            throws SQLException
    {
        return null;
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel)
            throws SQLException
    {
        return null;
    }

    @Override
    public InputStream getBinaryStream(String columnLabel)
            throws SQLException
    {
        return null;
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
    {}

    @Override
    public String getCursorName()
            throws SQLException
    {
        return null;
    }

    @Override
    public int findColumn(String columnLabel)
            throws SQLException
    {
        return 0;
    }

    @Override
    public Reader getCharacterStream(int columnIndex)
            throws SQLException
    {
        return null;
    }

    @Override
    public Reader getCharacterStream(String columnLabel)
            throws SQLException
    {
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel)
            throws SQLException
    {
        return getBigDecimalValue(columnLabel);
    }

    @Override
    public boolean isBeforeFirst()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean isAfterLast()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean isFirst()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean isLast()
            throws SQLException
    {
        return false;
    }

    @Override
    public void beforeFirst()
            throws SQLException
    {
    }

    @Override
    public void afterLast()
            throws SQLException
    {
    }

    @Override
    public boolean first()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean last()
            throws SQLException
    {
        return false;
    }

    @Override
    public int getRow()
            throws SQLException
    {
        return 0;
    }

    @Override
    public boolean absolute(int row)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean relative(int rows)
            throws SQLException
    {
        return false;
    }

    @Override
    public void setFetchDirection(int direction)
            throws SQLException
    {
    }

    @Override
    public int getFetchDirection()
            throws SQLException
    {
        return 0;
    }

    @Override
    public void setFetchSize(int rows)
            throws SQLException
    {
    }

    @Override
    public int getFetchSize()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getConcurrency()
            throws SQLException
    {
        return 0;
    }

    @Override
    public boolean rowUpdated()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean rowInserted()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean rowDeleted()
            throws SQLException
    {
        return false;
    }

    @Override
    public void updateNull(int columnIndex)
            throws SQLException
    {
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x)
            throws SQLException
    {
    }

    @Override
    public void updateByte(int columnIndex, byte x)
            throws SQLException
    {
    }

    @Override
    public void updateShort(int columnIndex, short x)
            throws SQLException
    {
    }

    @Override
    public void updateInt(int columnIndex, int x)
            throws SQLException
    {
    }

    @Override
    public void updateLong(int columnIndex, long x)
            throws SQLException
    {
    }

    @Override
    public void updateFloat(int columnIndex, float x)
            throws SQLException
    {
    }

    @Override
    public void updateDouble(int columnIndex, double x)
            throws SQLException
    {
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x)
            throws SQLException
    {
    }

    @Override
    public void updateString(int columnIndex, String x)
            throws SQLException
    {
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x)
            throws SQLException
    {
    }

    @Override
    public void updateDate(int columnIndex, Date x)
            throws SQLException
    {
    }

    @Override
    public void updateTime(int columnIndex, Time x)
            throws SQLException
    {
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x)
            throws SQLException
    {
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length)
            throws SQLException
    {
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length)
            throws SQLException
    {
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length)
            throws SQLException
    {
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength)
            throws SQLException
    {
    }

    @Override
    public void updateObject(int columnIndex, Object x)
            throws SQLException
    {
    }

    @Override
    public void updateNull(String columnLabel)
            throws SQLException
    {
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x)
            throws SQLException
    {
    }

    @Override
    public void updateByte(String columnLabel, byte x)
            throws SQLException
    {
    }

    @Override
    public void updateShort(String columnLabel, short x)
            throws SQLException
    {
    }

    @Override
    public void updateInt(String columnLabel, int x)
            throws SQLException
    {
    }

    @Override
    public void updateLong(String columnLabel, long x)
            throws SQLException
    {
    }

    @Override
    public void updateFloat(String columnLabel, float x)
            throws SQLException
    {
    }

    @Override
    public void updateDouble(String columnLabel, double x)
            throws SQLException
    {
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x)
            throws SQLException
    {
    }

    @Override
    public void updateString(String columnLabel, String x)
            throws SQLException
    {
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x)
            throws SQLException
    {
    }

    @Override
    public void updateDate(String columnLabel, Date x)
            throws SQLException
    {
    }

    @Override
    public void updateTime(String columnLabel, Time x)
            throws SQLException
    {
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x)
            throws SQLException
    {
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length)
            throws SQLException
    {
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length)
            throws SQLException
    {
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length)
            throws SQLException
    {
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength)
            throws SQLException
    {
    }

    @Override
    public void updateObject(String columnLabel, Object x)
            throws SQLException
    {
    }

    @Override
    public void insertRow()
            throws SQLException
    {
    }

    @Override
    public void updateRow()
            throws SQLException
    {
    }

    @Override
    public void deleteRow()
            throws SQLException
    {
    }

    @Override
    public void refreshRow()
            throws SQLException
    {
    }

    @Override
    public void cancelRowUpdates()
            throws SQLException
    {
    }

    @Override
    public void moveToInsertRow()
            throws SQLException
    {
    }

    @Override
    public void moveToCurrentRow()
            throws SQLException
    {
    }

    @Override
    public Statement getStatement()
            throws SQLException
    {
        return null;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map)
            throws SQLException
    {
        return null;
    }

    @Override
    public Ref getRef(int columnIndex)
            throws SQLException
    {
        return null;
    }

    @Override
    public Blob getBlob(int columnIndex)
            throws SQLException
    {
        return null;
    }

    @Override
    public Clob getClob(int columnIndex)
            throws SQLException
    {
        return null;
    }

    @Override
    public Array getArray(int columnIndex)
            throws SQLException
    {
        return null;
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map)
            throws SQLException
    {
        return null;
    }

    @Override
    public Ref getRef(String columnLabel)
            throws SQLException
    {
        return null;
    }

    @Override
    public Blob getBlob(String columnLabel)
            throws SQLException
    {
        return null;
    }

    @Override
    public Clob getClob(String columnLabel)
            throws SQLException
    {
        return null;
    }

    @Override
    public Array getArray(String columnLabel)
            throws SQLException
    {
        return null;
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal)
            throws SQLException
    {
        return null;
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal)
            throws SQLException
    {
        return null;
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal)
            throws SQLException
    {
        return null;
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal)
            throws SQLException
    {
        return null;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal)
            throws SQLException
    {
        return null;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal)
            throws SQLException
    {
        return null;
    }

    @Override
    public URL getURL(int columnIndex)
            throws SQLException
    {
        return null;
    }

    @Override
    public URL getURL(String columnLabel)
            throws SQLException
    {
        return null;
    }

    @Override
    public void updateRef(int columnIndex, Ref x)
            throws SQLException
    {
    }

    @Override
    public void updateRef(String columnLabel, Ref x)
            throws SQLException
    {
    }

    @Override
    public void updateBlob(int columnIndex, Blob x)
            throws SQLException
    {
    }

    @Override
    public void updateBlob(String columnLabel, Blob x)
            throws SQLException
    {
    }

    @Override
    public void updateClob(int columnIndex, Clob x)
            throws SQLException
    {
    }

    @Override
    public void updateClob(String columnLabel, Clob x)
            throws SQLException
    {
    }

    @Override
    public void updateArray(int columnIndex, Array x)
            throws SQLException
    {
    }

    @Override
    public void updateArray(String columnLabel, Array x)
            throws SQLException
    {
    }

    @Override
    public RowId getRowId(int columnIndex)
            throws SQLException
    {
        return null;
    }

    @Override
    public RowId getRowId(String columnLabel)
            throws SQLException
    {
        return null;
    }

    @Override
    public void updateRowId(int columnIndex, RowId x)
            throws SQLException
    {
    }

    @Override
    public void updateRowId(String columnLabel, RowId x)
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
    public boolean isClosed()
            throws SQLException
    {
        return false;
    }

    @Override
    public void updateNString(int columnIndex, String nString)
            throws SQLException
    {
    }

    @Override
    public void updateNString(String columnLabel, String nString)
            throws SQLException
    {
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob)
            throws SQLException
    {
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob)
            throws SQLException
    {
    }

    @Override
    public NClob getNClob(int columnIndex)
            throws SQLException
    {
        return null;
    }

    @Override
    public NClob getNClob(String columnLabel)
            throws SQLException
    {
        return null;
    }

    @Override
    public SQLXML getSQLXML(int columnIndex)
            throws SQLException
    {
        return null;
    }

    @Override
    public SQLXML getSQLXML(String columnLabel)
            throws SQLException
    {
        return null;
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject)
            throws SQLException
    {
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject)
            throws SQLException
    {
    }

    @Override
    public String getNString(int columnIndex)
            throws SQLException
    {
        return null;
    }

    @Override
    public String getNString(String columnLabel)
            throws SQLException
    {
        return null;
    }

    @Override
    public Reader getNCharacterStream(int columnIndex)
            throws SQLException
    {
        return null;
    }

    @Override
    public Reader getNCharacterStream(String columnLabel)
            throws SQLException
    {
        return null;
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length)
            throws SQLException
    {
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length)
            throws SQLException
    {
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length)
            throws SQLException
    {
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length)
            throws SQLException
    {
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length)
            throws SQLException
    {
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length)
            throws SQLException
    {
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length)
            throws SQLException
    {
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length)
            throws SQLException
    {
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length)
            throws SQLException
    {
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length)
            throws SQLException
    {
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length)
            throws SQLException
    {
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length)
            throws SQLException
    {
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length)
            throws SQLException
    {
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length)
            throws SQLException
    {
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x)
            throws SQLException
    {
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader)
            throws SQLException
    {
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x)
            throws SQLException
    {
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x)
            throws SQLException
    {
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x)
            throws SQLException
    {
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x)
            throws SQLException
    {
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x)
            throws SQLException
    {
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader)
            throws SQLException
    {
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream)
            throws SQLException
    {
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream)
            throws SQLException
    {
    }

    @Override
    public void updateClob(int columnIndex, Reader reader)
            throws SQLException
    {
    }

    @Override
    public void updateClob(String columnLabel, Reader reader)
            throws SQLException
    {
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader)
            throws SQLException
    {
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader)
            throws SQLException
    {
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type)
            throws SQLException
    {
        return null;
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type)
            throws SQLException
    {
        return null;
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

    //---------------------------------------------------Conversion Methods--------------------------------------------------------

    private double getDoubleValue(String columnLabel)
            throws SQLException
    {
        Object value = getObject(columnLabel);
        if (Objects.isNull(value)) {
            wasLastReturnNull = true;
            return 0;
        }
        wasLastReturnNull = false;
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        try {
            return Double.parseDouble(value.toString());
        }
        catch (NumberFormatException numberFormatException) {
            throw new SQLException("Unable to convert column '" + columnLabel + "' value '" + value + "' to double", numberFormatException);
        }
    }

    private BigDecimal getBigDecimalValue(String columnLabel)
            throws SQLException
    {
        Object value = getObject(columnLabel);
        if (Objects.isNull(value)) {
            wasLastReturnNull = true;
            return null;
        }
        wasLastReturnNull = false;
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        try {
            return new BigDecimal(value.toString());
        }
        catch (NumberFormatException numberFormatException) {
            throw new SQLException("Unable to convert column '" + columnLabel + "' value '" + value + "' to BigDecimal", numberFormatException);
        }
    }

    private Timestamp parseTimestampString(String columnLabel, String value)
            throws SQLException
    {
        String stringValue = value.trim();
        if (stringValue.isEmpty() || NULL_MARKER.equals(stringValue)) {
            wasLastReturnNull = true;
            return null;
        }

        String normalizedIsoValue = normalizeDateTimeSeparator(stringValue);
        try {
            return Timestamp.from(Instant.parse(normalizedIsoValue));
        }
        catch (DateTimeParseException ignored) {
        }
        try {
            return Timestamp.from(OffsetDateTime.parse(normalizedIsoValue).toInstant());
        }
        catch (DateTimeParseException ignored) {
        }
        try {
            return Timestamp.valueOf(stringValue.replace('T', ' '));
        }
        catch (IllegalArgumentException ignored) {
        }
        try {
            return Timestamp.valueOf(LocalDate.parse(stringValue).atStartOfDay());
        }
        catch (DateTimeParseException exception) {
            throw new SQLException("Unable to convert column '" + columnLabel + "' value '" + value + "' to Timestamp", exception);
        }
    }

    private String normalizeDateTimeSeparator(String value)
    {
        return value.length() > 10 && value.charAt(10) == ' '
                ? value.substring(0, 10) + "T" + value.substring(11)
                : value;
    }
}
