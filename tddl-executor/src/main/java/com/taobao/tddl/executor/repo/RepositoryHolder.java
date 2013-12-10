package com.taobao.tddl.executor.repo;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.utils.extension.ExtensionLoader;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.executor.spi.IRepositoryFactory;

public class RepositoryHolder {

    private final static Logger     logger     = LoggerFactory.getLogger(RepositoryHolder.class);
    public Map<String, IRepository> repository = new HashMap<String, IRepository>();

    public boolean containsKey(Object arg0) {

        return repository.containsKey(arg0);
    }

    public boolean containsValue(Object arg0) {
        return repository.containsValue(arg0);
    }

    public IRepository get(Object arg0) {

        return repository.get(arg0);
    }

    public IRepository getOrCreateRepository(String repoName) {

        if (get(repoName) != null) {
            return get(repoName);
        }

        synchronized (this) {
            if (get(repoName) == null) {
                IRepositoryFactory factory = getRepoFactory(repoName);

                IRepository repo = factory.buildRepository();

                if (repo == null) {
                    throw new TddlRuntimeException(repoNotFoundError.format(repoName));
                }

                repo.init();

                this.put(repoName, repo);
            }

        }

        return this.get(repoName);
    }

    public IRepository put(String arg0, IRepository arg1) {

        return repository.put(arg0, arg1);
    }

    public Set<Entry<String, IRepository>> entrySet() {
        return repository.entrySet();
    }

    public Map<String, IRepository> getRepository() {
        return repository;
    }

    public void setRepository(Map<String, IRepository> reponsitory) {
        this.repository = reponsitory;
    }

    public static final MessageFormat repoNotFoundError = new MessageFormat("repository is not loaded, name is: {0}");

    public IRepositoryFactory getRepoFactory(String repoName) {
        IRepositoryFactory factory = ExtensionLoader.load(IRepositoryFactory.class, repoName);

        if (factory == null) {
            throw new TddlRuntimeException(repoNotFoundError.format(repoName));
        }
        return factory;
    }
}
