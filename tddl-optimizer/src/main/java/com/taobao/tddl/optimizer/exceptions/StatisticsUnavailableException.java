package com.taobao.tddl.optimizer.exceptions;

public class StatisticsUnavailableException extends OptimizerException {

    private static final long serialVersionUID = -9046723340425196124L;

    public StatisticsUnavailableException(String errorCode, String errorDesc, Throwable cause){
        super(errorCode, errorDesc, cause);
    }

    public StatisticsUnavailableException(String errorCode, String errorDesc){
        super(errorCode, errorDesc);
    }

    public StatisticsUnavailableException(String errorCode, Throwable cause){
        super(errorCode, cause);
    }

    public StatisticsUnavailableException(String errorCode){
        super(errorCode);
    }

    public StatisticsUnavailableException(Throwable cause){
        super(cause);
    }

}
