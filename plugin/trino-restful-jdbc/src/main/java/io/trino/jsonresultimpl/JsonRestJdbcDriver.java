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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class JsonRestJdbcDriver
        implements Driver
{
    static String[] acceptedURLPatterns = new String[] {"jdbc:restmysql://", "jdbc:restpostgresql://", "jdbc:restoracle://", "jdbc:restsqlserver://", "jdbc:restmariadb://"};

    @Override
    public Connection connect(String url, Properties info)
    {
        return new JSONRSRestfulConnection(new org.json.JSONObject(info));
    }

    @Override
    public boolean acceptsURL(String url)
    {
        return acceptsURLPattern(url);
    }

    public static boolean acceptsURLPattern(String url)
    {
        if (url == null) {
            return false;
        }
        return Stream.of(acceptedURLPatterns).anyMatch(url::startsWith);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
            throws SQLException
    {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion()
    {
        return 0;
    }

    @Override
    public int getMinorVersion()
    {
        return 0;
    }

    @Override
    public boolean jdbcCompliant()
    {
        return false;
    }

    @Override
    public Logger getParentLogger()
            throws SQLFeatureNotSupportedException
    {
        return null;
    }
}
