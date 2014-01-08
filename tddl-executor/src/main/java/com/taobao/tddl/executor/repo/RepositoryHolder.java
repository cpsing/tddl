package com.taobao.tddl.executor.repo;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.utils.extension.ExtensionLoader;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.executor.spi.IRepositoryFactory;

public class RepositoryHolder {

    public static final MessageFormat repoNotFoundError = new MessageFormat("repository is not loaded, name is: {0}");
    private Map<String, IRepository>  repository        = new HashMap<String, IRepository>();

    public boolean containsKey(Object repoName) {
        return repository.containsKey(repoName);
    }

    public boolean containsValue(Object repoName) {
        return repository.containsValue(repoName);
    }

    public IRepository get(Object repoName) {
        return repository.get(repoName);
    }

    public IRepository getOrCreateRepository(String repoName, Map<String, String> properties) {
        if (get(repoName) != null) {
            return get(repoName);
        }

        synchronized (this) {
            if (get(repoName) == null) {
                IRepositoryFactory factory = getRepoFactory(repoName);
                IRepository repo = factory.buildRepository(properties);
                if (repo == null) {
                    throw new TddlRuntimeException(repoNotFoundError.format(repoName));
                }

                try {
                    repo.init();
                } catch (TddlException e) {
                    throw new TddlRuntimeException(e);
                }
                this.put(repoName, repo);
            }
        }

        return this.get(repoName);
    }

    private IRepositoryFactory getRepoFactory(String repoName) {
        IRepositoryFactory factory = ExtensionLoader.load(IRepositoryFactory.class, repoName);

        if (factory == null) {
            throw new TddlRuntimeException(repoNotFoundError.format(repoName));
        }
        return factory;
    }

    public IRepository put(String repoName, IRepository repo) {
        return repository.put(repoName, repo);
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

}
