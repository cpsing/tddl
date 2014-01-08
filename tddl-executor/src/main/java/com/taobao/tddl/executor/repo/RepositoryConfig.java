package com.taobao.tddl.executor.repo;

import java.util.HashMap;
import java.util.Map;

public class RepositoryConfig {

    public static final String  DEFAULT_TXN_ISOLATION = "DEFAULT_TXN_ISOLATION";
    public static final String  IS_TRANSACTIONAL      = "IS_TRANSACTIONAL";
    private Map<String, String> properties            = new HashMap();

    public String getProperty(String name) {
        return properties.get(name);
    }

    public void setProperty(String name, String value) {
        properties.put(name, value);
    }
}
