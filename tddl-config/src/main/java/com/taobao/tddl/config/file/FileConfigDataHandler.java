package com.taobao.tddl.config.file;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.config.ConfigDataHandler;
import com.taobao.tddl.config.ConfigDataListener;

public class FileConfigDataHandler implements ConfigDataHandler {

    private static final Logger           logger              = LoggerFactory.getLogger(FileConfigDataHandler.class);
    private final AtomicReference<String> data                = new AtomicReference<String>();
    private String                        pattern;
    private String                        directory;
    private String                        key;
    private String                        appName;
    private List<ConfigDataListener>      configDataListeners = new ArrayList<ConfigDataListener>();

    public FileConfigDataHandler(String appName, Executor executor, String pattern, String directory, String key,
                                 ConfigDataListener configDataListener){
        super();

        this.pattern = pattern;
        this.directory = directory;
        this.key = key;
        this.appName = appName;
        if (configDataListener != null) {

            this.configDataListeners.add(configDataListener);
        }
        if (executor == null) {
            throw new IllegalArgumentException("executor is null");
        }
        // this.executor.execute(new CheckerTask(data, pattern, directory, key,
        // configDataListeners,appName));
    }

    // TODO shenxun :这个不知道被谁注释掉了。目前不支持文件的重载了
    public static class CheckerTask implements Runnable {

        private AtomicReference<String>  data;
        private String                   pattern;
        private String                   directory;
        private String                   key;
        private List<ConfigDataListener> configDataListeners;
        private String                   appName;

        public CheckerTask(AtomicReference<String> data, String pattern, String directory, String key,
                           List<ConfigDataListener> configDataListeners, String appName){
            super();
            this.data = data;
            this.pattern = pattern;
            this.directory = directory;
            this.key = key;
            this.appName = appName;
            this.configDataListeners = configDataListeners;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    StringBuilder dataNew = getNewProperties(directory, key, pattern, appName);
                    if (!dataNew.toString().equalsIgnoreCase(data.get())) {// 配置变更啦
                        this.data.set(dataNew.toString());
                        for (ConfigDataListener cdl : configDataListeners) {
                            cdl.onDataRecieved(data.get());
                        }
                    }
                    try {
                        Thread.sleep(20000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }

    }

    @Override
    public String getData(long timeout) {
        try {
            StringBuilder dataNew = getNewProperties(directory, key, pattern, appName);
            this.data.set(dataNew.toString());
            return data.get();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static StringBuilder getNewProperties(String directory, String key, String pattern, String appName)
                                                                                                               throws IOException {
        StringBuilder dataNew = new StringBuilder();
        StringBuilder url = getUrlWithoutDiamondPattern(directory, key);
        InputStream in = null;
        try {
            in = GeneralUtil.getInputStream(url.toString());
        } catch (Exception e) {
            logger.error("", e);
        }
        if (in == null) {
            try {
                in = GeneralUtil.getInputStream(getUrlWithDiamondPattern(directory, key, pattern, appName).toString());
            } catch (Exception e) {
                logger.error("", e);
            }
        }
        if (in == null) {
            throw new IllegalArgumentException("can't find file on " + url + " . or on "
                                               + getUrlWithDiamondPattern(directory, key, pattern, appName).toString());
        }
        BufferedReader bf = new BufferedReader(new InputStreamReader(in));
        String temp;
        while ((temp = bf.readLine()) != null) {
            dataNew.append(temp).append(System.getProperty("line.separator"));
        }

        return dataNew;
    }

    private static StringBuilder getUrlWithoutDiamondPattern(String directory, String key) {
        StringBuilder url = new StringBuilder();
        if (directory != null) {
            url.append(directory);
        }
        url.append(key);
        return url;
    }

    private static StringBuilder getUrlWithDiamondPattern(String directory, String key, String pattern, String appName) {
        StringBuilder url = new StringBuilder();
        if (directory != null) {
            url.append(directory);
        }
        if (pattern != null) {
            url.append(pattern);
        }
        if (appName != null) {
            url.append(appName);
        }
        url.append(key);
        return url;
    }

    @Override
    public void addListener(ConfigDataListener configDataListener) {
        if (!configDataListeners.contains(configDataListener)) {
            configDataListeners.add(configDataListener);
        }
    }

}
