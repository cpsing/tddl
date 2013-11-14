package com.taobao.tddl.optimizer.exceptions;

import com.taobao.tddl.common.exception.TddlException;

/**
 * @author jianghang 2013-11-12 下午3:38:43
 * @since 5.1.0
 */
public class QueryException extends TddlException {

    private static final long serialVersionUID = 6432150590171245275L;

    public QueryException(String errorCode, String errorDesc, Throwable cause){
        super(errorCode, errorDesc, cause);
    }

    public QueryException(String errorCode, String errorDesc){
        super(errorCode, errorDesc);
    }

    public QueryException(String errorCode, Throwable cause){
        super(errorCode, cause);
    }

    public QueryException(String errorCode){
        super(errorCode);
    }

    public QueryException(Throwable cause){
        super(cause);
    }

}
