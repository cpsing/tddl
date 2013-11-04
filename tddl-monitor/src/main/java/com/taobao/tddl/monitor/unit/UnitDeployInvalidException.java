package com.taobao.tddl.monitor.unit;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @date 2013-5-8下午04:08:08
 */
public class UnitDeployInvalidException extends Exception {

    /**
     * 
     */
    private static final long serialVersionUID = 6338010335576673742L;

    public UnitDeployInvalidException(){
        super();
    }

    public UnitDeployInvalidException(String message, Throwable cause){
        super(message, cause);
    }

    public UnitDeployInvalidException(String message){
        super(message);
    }

    public UnitDeployInvalidException(Throwable cause){
        super(cause);
    }
}
