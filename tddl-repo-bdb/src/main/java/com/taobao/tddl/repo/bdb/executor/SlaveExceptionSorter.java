package com.taobao.tddl.repo.bdb.executor;

public class SlaveExceptionSorter<T> extends CommonExceptionSorter<T> {

    @Override
    protected boolean match(Integer errorcode) {
        /**
         * >= 30000 见ExceptionErrorCodeUtils
         */
        if (errorcode >= 30000) {
            return true;
        }
        return false;
    }
}
