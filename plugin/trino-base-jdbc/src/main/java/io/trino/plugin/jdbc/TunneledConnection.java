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

import io.trino.connectivity.SSHTunnelUtil.SSHTunnel;

import java.sql.Connection;
import java.sql.SQLException;

import static java.util.Objects.requireNonNull;

public class TunneledConnection
        extends ForwardingConnection
{
    private final Connection delegateConnection;
    private final SSHTunnel tunnel;

    public TunneledConnection(Connection delegate, SSHTunnel tunnel)
    {
        this.delegateConnection = requireNonNull(delegate, "delegate is null");
        this.tunnel = tunnel;
    }

    @Override
    protected Connection delegate()
    {
        return delegateConnection;
    }

    @Override
    public void close()
            throws SQLException
    {
        super.close();
        if (tunnel != null) {
            tunnel.readUnlock();
        }
    }
}
