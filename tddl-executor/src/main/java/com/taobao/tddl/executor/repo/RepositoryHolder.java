package com.taobao.tddl.executor.repo;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.executor.spi.Repository;

public class RepositoryHolder {

    private final static Logger    logger     = LoggerFactory.getLogger(RepositoryHolder.class);
    public Map<String, Repository> repository = new HashMap<String, Repository>();

    public boolean containsKey(Object arg0) {

        return repository.containsKey(arg0);
    }

    public boolean containsValue(Object arg0) {
        return repository.containsValue(arg0);
    }

    public Repository get(Object arg0) {

        return repository.get(arg0);
    }

    public Repository put(String arg0, Repository arg1) {

        return repository.put(arg0, arg1);
    }

    public Set<Entry<String, Repository>> entrySet() {
        return repository.entrySet();
    }

    public Map<String, Repository> getRepository() {
        return repository;
    }

    public void setRepository(Map<String, Repository> reponsitory) {
        this.repository = reponsitory;
    }

}
