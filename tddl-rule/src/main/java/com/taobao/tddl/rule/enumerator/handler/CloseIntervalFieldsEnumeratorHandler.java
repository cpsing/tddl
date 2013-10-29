package com.taobao.tddl.rule.enumerator.handler;

import java.util.Set;

import com.taobao.tddl.common.model.sqljep.Comparative;

public interface CloseIntervalFieldsEnumeratorHandler {

    /**
     * 穷举出从source，根据自增value和自增次数Times，将结果写入retValue参数中
     * 
     * @param source
     * @param retValue
     * @param cumulativeTimes
     * @param atomIncrValue
     */
    void processAllPassableFields(Comparative source, Set<Object> retValue, Integer cumulativeTimes,
                                  Comparable<?> atomIncrValue);

    /**
     * 穷举出从from到to中的所有值，根据自增value和自增次数Times，将结果写入retValue参数中
     * 
     * @param from
     * @param to
     */
    abstract void mergeFeildOfDefinitionInCloseInterval(Comparative from, Comparative to, Set<Object> retValue,
                                                        Integer cumulativeTimes, Comparable<?> atomIncrValue);

}
