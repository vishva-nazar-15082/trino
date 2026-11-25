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
package io.trino.connector;

import io.airlift.log.Logger;

import java.io.IOException;
import java.net.Authenticator;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class RuntimeProxySelector
        extends ProxySelector
{
    private static final Logger log = Logger.get(RuntimeProxySelector.class);

    private static final String SOCKS_PROXY_ENABLED = "socks.proxy.enabled";
    private static final String SOCKS_PROXY_HOST = ProductConfig.getConfigValueString("socks.proxy.host", "proxy", false);
    private static final Integer SOCKS_PROXY_PORT = ProductConfig.getIntConfigValue("socks.proxy.port", 1080, false);

    private static final String HTTP_PROXY_ENABLED = "http.proxy.enabled";
    private static final String HTTP_PROXY_HOST = ProductConfig.getConfigValueString("http.proxy.host", "proxy", false);
    private static final Integer HTTP_PROXY_PORT = ProductConfig.getIntConfigValue("http.proxy.port", 3128, false);

    private static final String[] NO_PROXY_HOSTS = loadNoProxyHosts();

    private static InetAddress socksProxyHostAddress;
    private static InetAddress httpProxyHostAddress;
    private static ProxySelector defaultProxySelector;
    private static Authenticator defaultAuthenticator;
    private static long lastAddressRefreshTime;

    static {
        refreshProxyHostAddressIfNeeded();
    }

    private static String[] loadNoProxyHosts()
    {
        String noProxyHosts = ProductConfig.getConfigValueString("no.proxy.host.list", null, false);
        if (noProxyHosts == null || noProxyHosts.isEmpty()) {
            return new String[0];
        }
        return noProxyHosts.split(",");
    }

    public static void setDefaultSelectorAndAuthenticator()
    {
        defaultProxySelector = ProxySelector.getDefault();
        ProxySelector.setDefault(new RuntimeProxySelector());
        defaultAuthenticator = Authenticator.getDefault();
        Authenticator.setDefault(new RuntimeProxyAuthenticator());
    }

    private static void refreshProxyHostAddressIfNeeded()
    {
        if (System.currentTimeMillis() <= lastAddressRefreshTime) {
            return;
        }
        socksProxyHostAddress = resolve(SOCKS_PROXY_HOST);
        httpProxyHostAddress = resolve(HTTP_PROXY_HOST);
        lastAddressRefreshTime = System.currentTimeMillis() + 24L * 60 * 60 * 1000;
    }

    private static InetAddress resolve(String host)
    {
        if (host == null) {
            return null;
        }
        try {
            return InetAddress.getByName(host);
        }
        catch (UnknownHostException ignored) {
            return null;
        }
    }

    @Override
    public List<Proxy> select(URI uri)
    {
        requireNonNull(uri, "uri is null");
        refreshProxyHostAddressIfNeeded();
        List<Proxy> proxyList = new ArrayList<>();
        String host = uri.getHost();
        if (host != null && matchesNoProxyHost(host)) {
            proxyList.add(Proxy.NO_PROXY);
            return proxyList;
        }
        if ("socket".equalsIgnoreCase(uri.getScheme())) {
            if (Boolean.FALSE.equals(ProductConfig.getBooleanConfigValue(SOCKS_PROXY_ENABLED, true, false)) && !internalIpCheck(host, true, uri.getPort())) {
                proxyList.add(Proxy.NO_PROXY);
                return proxyList;
            }
            proxyList.add(new Proxy(Proxy.Type.SOCKS, new InetSocketAddress(SOCKS_PROXY_HOST, portOrDefault(SOCKS_PROXY_PORT, 1080))));
        }
        else if (host != null && List.of("http", "https").contains(uri.getScheme())) {
            if (Boolean.FALSE.equals(ProductConfig.getBooleanConfigValue(HTTP_PROXY_ENABLED, true, false)) || internalIpCheck(host, false, uri.getPort())) {
                proxyList.add(Proxy.NO_PROXY);
                return proxyList;
            }
            proxyList.add(new Proxy(Proxy.Type.HTTP, new InetSocketAddress(HTTP_PROXY_HOST, portOrDefault(HTTP_PROXY_PORT, 3128))));
        }
        if (proxyList.isEmpty() && defaultProxySelector != null) {
            return defaultProxySelector.select(uri);
        }
        return proxyList;
    }

    private static boolean matchesNoProxyHost(String host)
    {
        for (String noProxyHost : NO_PROXY_HOSTS) {
            if (host.contains(noProxyHost)) {
                return true;
            }
        }
        return false;
    }

    private static int portOrDefault(Integer port, int defaultPort)
    {
        return port == null ? defaultPort : port;
    }

    @Override
    public void connectFailed(URI uri, SocketAddress socketAddress, IOException exception)
    {
        log.error(exception, "Proxy connect failed for %s", uri);
    }

    public static boolean internalIpCheck(String host, boolean throwException, int port)
    {
        try {
            InetAddress address = InetAddress.getByName(host);
            boolean internal = address.isSiteLocalAddress() || address.isLoopbackAddress();
            if (internal && isExcludedInternalAddress(address, port)) {
                internal = false;
            }
            if (internal && throwException) {
                throw new RuntimeException("Host is in internal network: " + host);
            }
            return internal;
        }
        catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean isExcludedInternalAddress(InetAddress address, int port)
    {
        if (address.getHostAddress().equals(ProductConfig.getCurrentNodeIp())) {
            return true;
        }
        if (socksProxyHostAddress != null && socksProxyHostAddress.equals(address) && Objects.equals(SOCKS_PROXY_PORT, port)) {
            return true;
        }
        return httpProxyHostAddress != null && httpProxyHostAddress.equals(address) && Objects.equals(HTTP_PROXY_PORT, port);
    }

    public static class RuntimeProxyAuthenticator
            extends Authenticator
    {
        static final String SOCKS_PROXY_USER_NAME = ProductConfig.getConfigValueString("socks.proxy.user", "", false);
        static final String SOCKS_PROXY_PASSWORD = ProductConfig.getConfigValueString("socks.proxy.password", "", false);
        static final String HTTP_PROXY_USER_NAME = ProductConfig.getConfigValueString("http.proxy.user", "", false);
        static final String HTTP_PROXY_PASSWORD = ProductConfig.getConfigValueString("http.proxy.password", "", false);

        @Override
        protected PasswordAuthentication getPasswordAuthentication()
        {
            PasswordAuthentication authentication = authenticationForRequest();
            if (authentication != null) {
                return authentication;
            }
            if (defaultAuthenticator != null) {
                return defaultAuthenticator.requestPasswordAuthenticationInstance(
                        getRequestingHost(),
                        getRequestingSite(),
                        getRequestingPort(),
                        getRequestingProtocol(),
                        getRequestingPrompt(),
                        getRequestingScheme(),
                        getRequestingURL(),
                        getRequestorType());
            }
            return super.getPasswordAuthentication();
        }

        private PasswordAuthentication authenticationForRequest()
        {
            if (matches(SOCKS_PROXY_HOST, socksProxyHostAddress, SOCKS_PROXY_PORT) && SOCKS_PROXY_USER_NAME != null && SOCKS_PROXY_PASSWORD != null) {
                return new PasswordAuthentication(SOCKS_PROXY_USER_NAME, SOCKS_PROXY_PASSWORD.toCharArray());
            }
            if (matches(HTTP_PROXY_HOST, httpProxyHostAddress, HTTP_PROXY_PORT) && HTTP_PROXY_USER_NAME != null && HTTP_PROXY_PASSWORD != null) {
                return new PasswordAuthentication(HTTP_PROXY_USER_NAME, HTTP_PROXY_PASSWORD.toCharArray());
            }
            return null;
        }

        private boolean matches(String proxyHost, InetAddress proxyAddress, Integer proxyPort)
        {
            if (proxyHost == null || proxyPort == null || !proxyPort.equals(getRequestingPort())) {
                return false;
            }
            if (proxyHost.equals(getRequestingHost())) {
                return true;
            }
            return proxyAddress != null && proxyAddress.getHostAddress().equals(getRequestingHost());
        }
    }
}
