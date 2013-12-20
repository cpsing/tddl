package com.taobao.tddl.repo.bdb.executor;

import java.util.Map;

public interface ExceptionSorter<T> {

    public static class ReturnVal<T> {

        public boolean                retryException;
        Map<T, String/* exception */> excludeKeys;

        public boolean isRetryException() {
            return retryException;
        }

        public ReturnVal<T> setRetryException(boolean retryException) {
            this.retryException = retryException;
            return this;
        }

        public Map<T, String> getExcludeKeys() {
            return excludeKeys;
        }

        public ReturnVal<T> setExcludeKeys(Map<T, String> excludeKeys) {
            this.excludeKeys = excludeKeys;
            return this;
        }

    }

    /**
     * 是否是个FatalException;
     * 
     * @param exception 对应的exception
     * @param excludeKeys 排除key,如果是fatal exception.那么excludeKeys就会被填入对应的key
     * @return
     */
    ReturnVal<T> isRetryException(String exception, Map<T, String/* exception */> excludeKeys, T t);
}
