package com.taobao.tddl.repo.bdb.executor;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.common.utils.ExceptionErrorCodeUtils;

public abstract class CommonExceptionSorter<T> implements ExceptionSorter<T> {

    private static final Log logger = LogFactory.getLog(CommonExceptionSorter.class);

    @Override
    public ReturnVal<T> isRetryException(String exception, Map<T, String> excludeKeys, T t) {
        if (exception == null) {
            return new ReturnVal<T>().setRetryException(false);
        }

        Integer errorcode = ExceptionErrorCodeUtils.getErrorCode(exception);
        if (errorcode == null) {
            logger.warn("exception doesn't has error code" + exception);
            return new ReturnVal<T>().setRetryException(false);
        } else {
            if (match(errorcode)) {
                if (excludeKeys == null) {
                    excludeKeys = new HashMap<T, String>();
                }
                excludeKeys.put(t, exception);

                return new ReturnVal<T>().setRetryException(true).setExcludeKeys(excludeKeys);
            }
            return new ReturnVal<T>().setRetryException(false);
        }

    }

    protected abstract boolean match(Integer errorcode);

}
