//Copyright(c) Taobao.com
package com.taobao.tddl.config.impl;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.config.ConfigDataHandler;
import com.taobao.tddl.config.ConfigDataHandlerFactory;
import com.taobao.tddl.config.ConfigDataListener;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 1.0
 * @since 1.6
 * @date 2011-1-11下午01:17:21
 */
@SuppressWarnings("rawtypes")
public class DefaultConfigDataHandlerFactory implements ConfigDataHandlerFactory {

    private static final Logger log                   = LoggerFactory.getLogger(DefaultConfigDataHandlerFactory.class);
    private static final String HANDLER_CLASS         = "config.handler.constructor.name";
    private static final String DEFAULT_HANDLER_CLASS = "com.taobao.tddl.config.diamond.DiamondConfigDataHandler";

    private static String       propertyFile          = "remote-config.properties";
    private static String       handlerClassName;
    private static Class        handlerClassObj;
    private static Constructor  handlerConstructor;
    private static Properties   prop;

    static {
        findSpecifiedConfigHandlerClass();
        createConstuctFromClassName();
    }

    private static void findSpecifiedConfigHandlerClass() {
        ClassLoader currentCL = getBaseClassLoader();
        InputStream resource;
        for (;;) {
            if (currentCL != null) {
                resource = currentCL.getResourceAsStream(propertyFile);
            } else {
                resource = ClassLoader.getSystemResourceAsStream(propertyFile);
                break;
            }

            if (null != resource) {
                break;
            } else {
                currentCL = currentCL.getParent();
            }
        }

        if (null != resource) {
            prop = new Properties();
            try {
                prop.load(resource);
                handlerClassName = prop.getProperty(HANDLER_CLASS);
                if (null == handlerClassName || "".equals(handlerClassName)) {
                    handlerClassName = DEFAULT_HANDLER_CLASS;
                }
            } catch (IOException e) {
                log.error("properties can not load " + propertyFile);
            }
        } else {
            handlerClassName = DEFAULT_HANDLER_CLASS;
        }
    }

    @SuppressWarnings("unchecked")
    private static void createConstuctFromClassName() {
        ClassLoader currentCL = getBaseClassLoader();
        handlerClassObj = loadClass(handlerClassName, currentCL);
        if (null == handlerClassObj) {
            throw new IllegalArgumentException("can not get handler class:" + handlerClassName);
        }

        try {
            handlerConstructor = handlerClassObj.getConstructor();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
    }

    private static Class loadClass(String className, ClassLoader currentCL) {
        log.info("Trying to load '" + className);
        try {
            Class clazz = currentCL.loadClass(handlerClassName);
            if (clazz != null) {
                return clazz;
            }
        } catch (ClassNotFoundException e) {
            log.error("can not load the class ");
        }

        return null;

    }

    private static boolean useTCCL = true;

    private static ClassLoader getBaseClassLoader() {
        ClassLoader thisClassLoader = DefaultConfigDataHandlerFactory.class.getClassLoader();
        if (useTCCL == false) {
            return thisClassLoader;
        }
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        ClassLoader baseClassLoader = getLowestClassLoader(contextClassLoader, thisClassLoader);
        return baseClassLoader;
    }

    private static ClassLoader getLowestClassLoader(ClassLoader c1, ClassLoader c2) {
        if (c1 == null) return c2;

        if (c2 == null) return c1;

        ClassLoader current;

        current = c1;
        while (current != null) {
            if (current == c2) return c1;
            current = current.getParent();
        }

        current = c2;
        while (current != null) {
            if (current == c1) return c2;
            current = current.getParent();
        }

        return null;
    }

    public static String objectId(Object o) {
        if (o == null) {
            return "null";
        } else {
            // 这里这个System.identityHashCode只会在初始化时调用一次，所以
            // 其可能存在的问题影响并不大。
            return o.getClass().getName() + "@" + System.identityHashCode(o);
        }
    }

    @Override
    public ConfigDataHandler getConfigDataHandler(String dataId, String unitName) {
        return this.getConfigDataHandlerWithListener(dataId, null, unitName);
    }

    @Override
    public ConfigDataHandler getConfigDataHandlerWithListener(String dataId, ConfigDataListener configDataListener,
                                                              String unitName) {
        List<ConfigDataListener> configDataListenerList = new ArrayList<ConfigDataListener>();
        configDataListenerList.add(configDataListener);
        return this.getConfigDataHandlerWithFullConfig(dataId,
            configDataListenerList,
            null,
            new HashMap<String, String>(),
            unitName);
    }

    // 不能更换listenerList的引用，避免后续对list的修改无效
    // 以下做法是被禁止的
    // List result = new List
    // result.add
    // return result
    protected List<ConfigDataListener> clearNullListener(List<ConfigDataListener> configDataListenerList) {
        for (int index = 0; index < configDataListenerList.size(); index++) {
            configDataListenerList.remove(null);
        }
        return configDataListenerList;
    }

    @Override
    public ConfigDataHandler getConfigDataHandlerWithFullConfig(String dataId,
                                                                List<ConfigDataListener> configDataListenerList,
                                                                Executor executor, Map<String, String> config,
                                                                String unitName) {
        ConfigDataHandler instance = newConfigDataHandler();
        if (instance == null) return null;

        Map<String, Object> configMap = getConfigMap(config);

        instance.init(dataId, clearNullListener(configDataListenerList), configMap, unitName);
        return instance;
    }

    protected Map<String, Object> getConfigMap(Map<String, String> config) {
        Map<String, Object> configMap = new HashMap<String, Object>();
        if (config != null) {
            configMap.putAll(config);
        }
        configMap = putDefaultConfMap(configMap);
        return configMap;
    }

    @SuppressWarnings("unchecked")
    protected Map<String, Object> putDefaultConfMap(Map<String, Object> configMap) {
        if (prop != null) {
            configMap.putAll((Map) prop);
        }
        return configMap;
    }

    protected ConfigDataHandler newConfigDataHandler() {
        ConfigDataHandler result = null;
        try {
            result = (ConfigDataHandler) handlerConstructor.newInstance();
        } catch (IllegalArgumentException e) {
            log.error("illegal arguments!", e);
        } catch (InstantiationException e) {
            log.error("handler init error!", e);
        } catch (IllegalAccessException e) {
            log.error("securty limit,handler can not be init!", e);
        } catch (InvocationTargetException e) {
            log.error("constructor invode error!", e);
        }
        return result;
    }
}
