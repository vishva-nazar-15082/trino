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

import java.sql.Types;
import java.util.HashMap;

public class JSONResultImpConstants
{
    public static final String AUTHENTICATION_URL = "AUTHENTICATION_URL";
    public static final String AUTHENTICATION_KEY = "AUTHENTICATION_KEY";
    public static final String AUTHENTICATION_RESOURCE = "AUTHENTICATION_RESOURCE";
    public static final String GET_RESULT_URL = "GET_RESULT_URL";
    public static final String GET_NEXT_BATCH_URL = "GET_NEXT_BATCH_URL";
    public static final String CURRENT_BATCH_KEY = "CURRENT_BATCH_KEY";
    public static final String NEXT_BATCH_KEY = "NEXT_BATCH_KEY";
    public static final String ADDITIONAL_DATA = "ADDITIONAL_DATA";
    public static final String SQL_QUERY = "SQL_QUERY";
    public static final String ROWS_PER_BATCH = "ROWS_PER_BATCH";
    public static final String RESULT = "RESULT";
    public static final String RESULT_DATA = "DATA";
    public static final String RESULT_ID_NAME_MAPPING = "ID_NAME_MAPPING";
    public static final String RESULT_ID_TYPE_MAPPING = "ID_TYPE_MAPPING";
    public static final String RESULT_IS_JAVA_COL_TYPE = "IS_JAVA_COL_TYPE";
    public static final String NULL_MARKER = "__NULL__";
    public static final String HAS_MORE = "HAS_MORE";
    public static final String SESSION_ID = "SESSION_ID";
    public static final String PREFETCH_THRESHOLD_PERCENT = "PREFETCH_THRESHOLD_PERCENT";
    public static final String DEFAULT_PREFETCH_THRESHOLD_PERCENT = "75";
    public static final String RESULT_TYPE = "RESULT_TYPE";
    public static final String META_TABLES = "META_TABLES";
    public static final String META_SCHEMAS = "META_SCHEMAS";
    public static final String META_COLUMNS = "META_COLUMNS";
    public static final HashMap<String, String> JAVA_DATATYPE_NAME_MAP = new HashMap<>()
    {
        {
            put("bool", "BOOLEAN");
            put("int", "INT");
            put("float", "DECIMAL");
            put("Decimal", "DECIMAL");
            put("str", "VARCHAR");
            put("bytes", "VARBINARY");
            put("date", "DATE");
            put("time", "TIME");
            put("datetime", "TIMESTAMP");
            put("UUID", "VARCHAR");
        }
    };

    public static final HashMap<String, Integer> JAVA_DATATYPE_MAP = new HashMap<>()
    {
        {
            put("bool", Types.BOOLEAN);
            put("int", Types.INTEGER);
            put("float", Types.DECIMAL);
            put("Decimal", Types.DECIMAL);
            put("str", Types.VARCHAR);
            put("bytes", Types.VARBINARY);
            put("date", Types.DATE);
            put("time", Types.TIME);
            put("datetime", Types.TIMESTAMP);
            put("UUID", Types.VARCHAR);
        }
    };

    public enum RequestMethod
    {
        GET, POST, PUT, DELETE
    }

    public static class ParamHolder
    {
        String value = "null";

        public String getValue()
        {
            return value;
        }

        public void setValue(String value)
        {
            this.value = value;
        }
    }
}
