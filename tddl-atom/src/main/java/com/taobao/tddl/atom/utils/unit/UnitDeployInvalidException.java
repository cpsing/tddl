package com.taobao.tddl.atom.utils.unit;

import com.taobao.tddl.common.exception.TddlException;

/**
 * @author <a href="junyu@taobao.com">junyu</a>
 * @date 2013-5-8 04:08:08
 */
public class UnitDeployInvalidException extends TddlException {

    private static final long serialVersionUID = 6338010335576673742L;

    public UnitDeployInvalidException(String errorCode, String errorDesc, Throwable cause){
        super(errorCode, errorDesc, cause);
    }

    public UnitDeployInvalidException(String errorCode, String errorDesc){
        super(errorCode, errorDesc);
    }

    public UnitDeployInvalidException(String errorCode, Throwable cause){
        super(errorCode, cause);
    }

    public UnitDeployInvalidException(String errorCode){
        super(errorCode);
    }

    public UnitDeployInvalidException(Throwable cause){
        super(cause);
    }
}
