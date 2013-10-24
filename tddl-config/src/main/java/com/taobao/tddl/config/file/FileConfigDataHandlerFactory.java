package com.taobao.tddl.config.file;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.taobao.tddl.config.ConfigDataHandler;
import com.taobao.tddl.config.ConfigDataHandlerFactory;
import com.taobao.tddl.config.ConfigDataListener;

public class FileConfigDataHandlerFactory implements ConfigDataHandlerFactory {

    private String                                                    directory = "";
    private final static ConcurrentHashMap<String, ConfigDataHandler> filePath  = new ConcurrentHashMap<String, ConfigDataHandler>();

    public FileConfigDataHandlerFactory(String directory, Executor executor){
        super();
        this.directory = directory;
        if (executor != null) {
            this.executor = executor;
        } else {
            this.executor = default_Executor;
        }
    }

    private Executor              executor         = null;

    private final static Executor default_Executor = Executors.newCachedThreadPool(new ThreadFactory() {

                                                       @Override
                                                       public Thread newThread(Runnable r) {
                                                           Thread thd = new Thread(r);
                                                           thd.setName("configure file checker");
                                                           return thd;
                                                       }
                                                   });

    public String getDirectory() {
        return directory;
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }

    public Executor getExecutor() {
        return executor;
    }

    public void setExecutor(Executor executor) {
        this.executor = executor;
    }

    @Override
    public ConfigDataHandler getConfigDataHandler(String appName, String dataId, ConfigDataListener configDataListener) {
        String key = combineDataKey(appName, dataId);
        ConfigDataHandler configDataHandler = filePath.get(key);
        if (configDataHandler == null) {
            synchronized (this) {
                configDataHandler = filePath.get(key);
                // dcl
                if (configDataHandler == null) {
                    configDataHandler = new FileConfigDataHandler(appName,
                        executor,
                        ConfigDataHandlerFactory.configure_prefix,
                        directory,
                        dataId,
                        configDataListener);
                    ConfigDataHandler tempCdh = filePath.putIfAbsent(key, configDataHandler);
                    if (tempCdh != null) {
                        configDataHandler = tempCdh;
                    }
                }

            }
        }
        configDataHandler.addListener(configDataListener);
        return configDataHandler;
    }

    private final String combineDataKey(String appName, String dataId) {
        return appName + "_" + dataId;
    }

    @Override
    public ConfigDataHandler getConfigDataHandler(String dataId, ConfigDataListener configDataListener) {
        // TODO Auto-generated method stub
        return null;
    }
}
