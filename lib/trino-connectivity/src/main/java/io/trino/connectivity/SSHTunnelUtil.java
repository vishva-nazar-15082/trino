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
package io.trino.connectivity;

import io.airlift.log.Logger;
import io.trino.connector.ProductConfig;
import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.proxy.ProxyData;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.common.forward.PortForwardingTracker;
import org.apache.sshd.common.util.net.SshdSocketAddress;

import java.lang.ref.WeakReference;
import java.net.Authenticator;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.URI;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.Objects.requireNonNull;

public final class SSHTunnelUtil
{
    private static final Logger log = Logger.get(SSHTunnelUtil.class);
    private static final AtomicBoolean TUNNEL_CLEANER_RUNNING = new AtomicBoolean();
    private static final Set<WeakReference<SSHTunnel>> ALIVE_TUNNELS = ConcurrentHashMap.newKeySet();

    public static final SSHTunnelCredential LOCAL_TUNNEL_CREDENTIAL = new SSHTunnelCredential(
            ProductConfig.getConfigValueString("ssh.tunnel.user", null, false),
            ProductConfig.getConfigValueString("ssh.tunnel.password", null, false),
            ProductConfig.getConfigValueString("ssh.tunnel.host", null, false),
            ProductConfig.getIntConfigValue("ssh.tunnel.port", 22, false));

    private SSHTunnelUtil()
    {
    }

    public static SSHTunnel getTunnel(String host, int port, SSHTunnelCredential tunnelCredential)
            throws Exception
    {
        SshClient client = SshClient.setUpDefaultClient();
        client.start();
        if (Boolean.TRUE.equals(ProductConfig.getBooleanConfigValue("ssh.tunnel.proxy.enabled", false, false))) {
            client.setProxyDataFactory(remoteAddress -> proxyData(remoteAddress));
        }
        try {
            InetAddress address = InetAddress.getByName(host);
            if (address.getHostAddress().equals(ProductConfig.getCurrentNodeIp())) {
                throw new IllegalArgumentException("Tunnel host cannot be the same as the current node IP");
            }
            SSHTunnelCredential credential = requireNonNull(tunnelCredential, "tunnelCredential is null");
            ClientSession session = client.connect(credential.username(), credential.host(), credential.port())
                    .verify(5, TimeUnit.SECONDS)
                    .getSession();
            session.addPasswordIdentity(credential.password());
            session.auth().verify(5, TimeUnit.SECONDS);
            SshdSocketAddress localAddress = new SshdSocketAddress(ProductConfig.getCurrentNodeIp(), 0);
            SshdSocketAddress remoteAddress = new SshdSocketAddress(host, port);
            PortForwardingTracker tracker = session.createLocalPortForwardingTracker(localAddress, remoteAddress);
            SSHTunnel sshTunnel = new SSHTunnel(client, session, tracker);
            ALIVE_TUNNELS.add(new WeakReference<>(sshTunnel));
            startCleaner();
            return sshTunnel;
        }
        catch (Exception e) {
            log.error(e, "Exception in creating tunnel");
            client.stop();
            throw e;
        }
    }

    private static ProxyData proxyData(InetSocketAddress remoteAddress)
    {
        URI uri = URI.create("socket://" + remoteAddress.getHostString() + ":" + remoteAddress.getPort());
        Proxy proxy = ProxySelector.getDefault().select(uri).get(0);
        if (!(proxy.address() instanceof InetSocketAddress proxyAddress)) {
            return new ProxyData(proxy);
        }
        PasswordAuthentication passwordAuthentication = Authenticator.requestPasswordAuthentication(
                proxyAddress.getHostString(),
                proxyAddress.getAddress(),
                proxyAddress.getPort(),
                "SOCKS5",
                "Password For Tunnel",
                uri.getScheme());
        if (passwordAuthentication == null) {
            return new ProxyData(proxy);
        }
        return new ProxyData(proxy, passwordAuthentication.getUserName(), passwordAuthentication.getPassword());
    }

    private static void startCleaner()
    {
        if (!TUNNEL_CLEANER_RUNNING.compareAndSet(false, true)) {
            return;
        }
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "ssh-tunnel-cleaner");
            thread.setDaemon(true);
            return thread;
        });
        executor.scheduleAtFixedRate(SSHTunnelUtil::closeExpiredTunnels, 2, 60, TimeUnit.SECONDS);
    }

    private static void closeExpiredTunnels()
    {
        ALIVE_TUNNELS.removeIf(reference -> reference.get() == null);
        for (WeakReference<SSHTunnel> tunnelReference : ALIVE_TUNNELS) {
            SSHTunnel tunnel = tunnelReference.get();
            if (tunnel != null) {
                tunnel.closeIfExpired();
            }
        }
    }

    public static final class SSHTunnel
    {
        private final SshClient client;
        private final ClientSession session;
        private final PortForwardingTracker tracker;
        private final long expireTime;
        private final ReentrantReadWriteLock lock;

        public SSHTunnel(SshClient client, ClientSession session, PortForwardingTracker tracker)
        {
            this.client = requireNonNull(client, "client is null");
            this.session = requireNonNull(session, "session is null");
            this.tracker = requireNonNull(tracker, "tracker is null");
            Long expiryMillis = ProductConfig.getLongConfigValue("ssh.tunnel.expiry", 900_000L, true);
            this.expireTime = System.currentTimeMillis() + requireNonNull(expiryMillis, "ssh.tunnel.expiry is null");
            this.lock = new ReentrantReadWriteLock(true);
        }

        public int getBoundPort()
        {
            return tracker.getBoundAddress().getPort();
        }

        public String getBoundHost()
        {
            return tracker.getBoundAddress().getHostName();
        }

        public boolean isExpired()
        {
            return expireTime <= System.currentTimeMillis();
        }

        public ReentrantReadWriteLock lock()
        {
            if (isExpired() || isClosed()) {
                throw new IllegalStateException("Tunnel is closed or expired");
            }
            return lock;
        }

        public void readUnlock()
        {
            lock.readLock().unlock();
        }

        public void closeIfExpired()
        {
            if (!isExpired()) {
                return;
            }
            lock.writeLock().lock();
            try {
                if (!isClosed()) {
                    close();
                }
            }
            catch (Exception ignored) {
                // A failed close should not stop the cleaner from visiting the remaining tunnels.
            }
            finally {
                lock.writeLock().unlock();
            }
        }

        public void close()
                throws Exception
        {
            try {
                tracker.close();
                session.close();
            }
            catch (Exception e) {
                log.error(e, "Exception in closing SSH sessions");
            }
            client.stop();
        }

        public boolean isClosed()
        {
            return client.isClosed();
        }
    }

    public static final class SSHTunnelCredential
    {
        private final String username;
        private final String password;
        private final String host;
        private final int port;

        public SSHTunnelCredential(String username, String password, String host, Integer port)
        {
            this.username = username;
            this.password = password;
            this.host = host;
            this.port = port == null ? 22 : port;
        }

        public String username()
        {
            return username;
        }

        public String password()
        {
            return password;
        }

        public String host()
        {
            return host;
        }

        public int port()
        {
            return port;
        }

        public boolean isValid()
        {
            return username != null && password != null && host != null;
        }
    }
}
