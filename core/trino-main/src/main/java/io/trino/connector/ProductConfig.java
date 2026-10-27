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
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.util.Objects.requireNonNull;

public final class ProductConfig
{
    private static final Logger log = Logger.get(ProductConfig.class);

    public static final String CONFIG_FILE_NAME = "etc/" + System.getProperty("trino.runtime.config-file", "runtime_config.properties");
    public static final String CONFIG_FILE_NAME_PATCH = "etc/" + System.getProperty("trino.runtime.patch.config-file", "runtime_config_patch.properties");
    public static final String CONFIG_FILE_NAME_PATCH_LOCK = "etc/" + System.getProperty("trino.runtime.config.lock-file", "runtime_config_patch.lock");

    private static final ConcurrentHashMap<String, Object> CONFIGS = new ConcurrentHashMap<>();
    private static volatile String currentNodeIp;

    private ProductConfig()
    {
    }

    public static void setCurrentNodeIp(String currentNodeIp)
    {
        ProductConfig.currentNodeIp = currentNodeIp;
    }

    public static String getCurrentNodeIp()
    {
        return currentNodeIp;
    }

    public static void initializeRuntimeConfig(String file)
            throws IOException
    {
        Path path = Path.of(file);
        if (!Files.isRegularFile(path)) {
            log.warn("Runtime config file %s is not present", file);
            return;
        }
        Properties properties = new Properties();
        try (InputStream inputStream = Files.newInputStream(path)) {
            properties.load(inputStream);
        }
        for (String key : properties.stringPropertyNames()) {
            CONFIGS.put(key, parseConfigValue(properties.getProperty(key)));
        }
    }

    private static Object parseConfigValue(String value)
    {
        try {
            return Long.parseLong(value);
        }
        catch (NumberFormatException ignored) {
            if ("true".equalsIgnoreCase(value)) {
                return Boolean.TRUE;
            }
            if ("false".equalsIgnoreCase(value)) {
                return Boolean.FALSE;
            }
            return value;
        }
    }

    public static Object getConfigValue(String configKey, Object defaultValue, Boolean required)
    {
        Object configValue = CONFIGS.getOrDefault(configKey, defaultValue);
        return Boolean.TRUE.equals(required) ? requireNonNull(configValue, "Value is not present for key: " + configKey) : configValue;
    }

    public static String getConfigValueString(String configKey, String defaultValue, Boolean required)
    {
        try {
            Object configValue = getConfigValue(configKey, defaultValue, required);
            if (configValue == null || configValue instanceof String) {
                return (String) configValue;
            }
            throw new IllegalArgumentException("Config value for " + configKey + " is not a string");
        }
        catch (RuntimeException e) {
            if (Boolean.TRUE.equals(required)) {
                throw e;
            }
            return null;
        }
    }

    public static Integer getIntConfigValue(String configKey, Integer defaultValue, Boolean required)
    {
        try {
            Object configValue = getConfigValue(configKey, defaultValue, required);
            if (configValue == null) {
                return null;
            }
            if (configValue instanceof Number number) {
                return number.intValue();
            }
            return Integer.valueOf(configValue.toString());
        }
        catch (RuntimeException e) {
            if (Boolean.TRUE.equals(required)) {
                throw e;
            }
            return null;
        }
    }

    public static Long getLongConfigValue(String configKey, Long defaultValue, Boolean required)
    {
        try {
            Object configValue = getConfigValue(configKey, defaultValue, required);
            if (configValue == null) {
                return null;
            }
            if (configValue instanceof Number number) {
                return number.longValue();
            }
            return Long.valueOf(configValue.toString());
        }
        catch (RuntimeException e) {
            if (Boolean.TRUE.equals(required)) {
                throw e;
            }
            return null;
        }
    }

    public static Boolean getBooleanConfigValue(String configKey, Boolean defaultValue, Boolean required)
    {
        try {
            Object configValue = getConfigValue(configKey, defaultValue, required);
            if (configValue == null) {
                return null;
            }
            if (configValue instanceof Boolean booleanValue) {
                return booleanValue;
            }
            return Boolean.valueOf(configValue.toString());
        }
        catch (RuntimeException e) {
            if (Boolean.TRUE.equals(required)) {
                throw e;
            }
            return null;
        }
    }

    public static void startPatchConfigFileListener()
    {
        Thread listener = new Thread(ProductConfig::watchPatchConfig);
        listener.setName("runtime-config-listener");
        listener.setDaemon(true);
        listener.start();
    }

    private static void watchPatchConfig()
    {
        Path targetFile = Path.of(CONFIG_FILE_NAME_PATCH_LOCK);
        Path patchFile = Path.of(CONFIG_FILE_NAME_PATCH);
        try {
            initializeRuntimeConfig(CONFIG_FILE_NAME_PATCH);
            Files.deleteIfExists(targetFile);
            Files.deleteIfExists(patchFile);
        }
        catch (IOException e) {
            log.error(e, "Exception reading runtime config file on startup");
        }
        Path parentDir = Path.of("etc");
        if (!Files.isDirectory(parentDir)) {
            log.warn("Runtime config directory %s is not present; patch listener not started", parentDir);
            return;
        }

        try (WatchService watcher = FileSystems.getDefault().newWatchService()) {
            parentDir.register(watcher, ENTRY_CREATE);
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    WatchKey key = watcher.take();
                    for (WatchEvent<?> event : key.pollEvents()) {
                        if (event.context().equals(targetFile.getFileName()) && event.kind() == ENTRY_CREATE) {
                            log.info("Runtime config patch lock created");
                            long maxTime = System.currentTimeMillis() + 60_000;
                            while (!Files.exists(patchFile)) {
                                if (maxTime < System.currentTimeMillis()) {
                                    break;
                                }
                                Thread.sleep(1000);
                            }
                            initializeRuntimeConfig(CONFIG_FILE_NAME_PATCH);
                            log.info("Deleting runtime config patch and lock files");
                            Files.deleteIfExists(targetFile);
                            Files.deleteIfExists(patchFile);
                        }
                    }
                    key.reset();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                catch (Exception e) {
                    log.error(e, "Exception reading runtime config patch");
                }
                try {
                    Thread.sleep(5000);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        catch (Exception e) {
            log.error(e, "Exception initializing runtime config listener");
        }
    }
}
