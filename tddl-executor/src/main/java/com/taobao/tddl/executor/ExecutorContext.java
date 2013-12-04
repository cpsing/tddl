package com.taobao.tddl.executor;

import com.taobao.tddl.common.utils.thread.ThreadLocalMap;
import com.taobao.tddl.executor.repo.RepositoryHolder;

/**
 * @author mengshi.sunmengshi 2013-12-4 下午6:16:32
 * @since 5.1.0
 */
public class ExecutorContext {

    private static final String EXECUTOR_CONTEXT_KEY = "_executor_context_";

    private RepositoryHolder    repositoryHolder     = null;

    public static ExecutorContext getContext() {
        return (ExecutorContext) ThreadLocalMap.get(EXECUTOR_CONTEXT_KEY);
    }

    public static void setContext(ExecutorContext context) {
        ThreadLocalMap.put(EXECUTOR_CONTEXT_KEY, context);
    }

    public RepositoryHolder getRepositoryHolder() {
        return repositoryHolder;
    }

    public void setRepositoryHolder(RepositoryHolder repositoryHolder) {
        this.repositoryHolder = repositoryHolder;
    }

}
