package com.taobao.tddl.repo.bdb.executor;

public class MasterExceptionSorter<T> extends CommonExceptionSorter<T> {

    @Override
    protected boolean match(Integer errorcode) {
        if (errorcode >= 20000) return true;
        return false;
    }
}
