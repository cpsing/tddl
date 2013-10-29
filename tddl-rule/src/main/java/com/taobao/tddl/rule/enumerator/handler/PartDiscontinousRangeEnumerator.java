package com.taobao.tddl.rule.enumerator.handler;

import java.util.Set;

import com.taobao.tddl.common.model.sqljep.Comparative;

public abstract class PartDiscontinousRangeEnumerator implements CloseIntervalFieldsEnumeratorHandler {

    /**
     * 一次自增
     * 
     * @param source
     * @param atomIncVal
     * @return
     */
    protected abstract Comparable getOneStep(Comparable source, Comparable atomIncVal);

    /**
     * 根据不同数据的最小单位将>变为>=
     * 
     * @param to
     * @return
     */
    protected abstract Comparative changeGreater2GreaterOrEq(Comparative from);

    /**
     * 根据不同数据的最小单位将<变为<=
     * 
     * @param to
     * @return
     */
    protected abstract Comparative changeLess2LessOrEq(Comparative to);

    /**
     * 如果输入的范围大于range.size() * atomIncrementvalue的值，那么就可以做短路优化
     * 
     * @param from 只有<=情况下的form值
     * @param to 只有>=情况下的to 值
     * @param range
     * @param atomIncrementValue
     * @return
     */
    protected abstract boolean inputCloseRangeGreaterThanMaxFieldOfDifination(Comparable from, Comparable to,
                                                                              Integer cumulativeTimes,
                                                                              Comparable<?> atomIncrValue);

    public void mergeFeildOfDefinitionInCloseInterval(Comparative from, Comparative to, Set<Object> retValue,
                                                      Integer cumulativeTimes, Comparable<?> atomIncrValue) {
        if (cumulativeTimes == null || atomIncrValue == null) {
            throw new IllegalArgumentException("当原子增参数或叠加参数为空时，不支持在sql中使用范围选择，如id>? and id<?");
        }
        from = changeGreater2GreaterOrEq(from);
        to = changeLess2LessOrEq(to);

        Comparable fromComparable = from.getValue();
        Comparable toComparable = to.getValue();

        if (inputCloseRangeGreaterThanMaxFieldOfDifination(fromComparable, toComparable, cumulativeTimes, atomIncrValue)) {
            // 如果所取得范围大于非连续函数的一个变动周期。直接断路掉,并且全取
            if (retValue != null) {
                processAllPassableFields(from, retValue, cumulativeTimes, atomIncrValue);
                return;
            } else {
                throw new IllegalArgumentException("待写入的参数set为null");
            }
        }

        if (fromComparable.compareTo(toComparable) == 0) {
            // 如果转变为>=和<=得情况下，俩值相等了，那么直接返回。
            retValue.add(fromComparable);
            return;
        }

        int rangeSize = cumulativeTimes;
        retValue.add(fromComparable);
        Comparable enumedFoD = fromComparable;
        for (int i = 0; i < rangeSize; i++) {
            enumedFoD = getOneStep(enumedFoD, atomIncrValue);
            int compareResult = enumedFoD.compareTo(toComparable);
            if (compareResult == 0) {
                // 枚举值等于to的值，简单的把to的值放到枚举数列里。返回
                retValue.add(toComparable);
                return;
            } else if (compareResult > 0) {
                // 枚举值大于to得值,按月分库的情况下也需要把最后一个月加上，其他情况会多算一个库
                // 这样做，在最后一天的时候会有可能出现两个值，第一个值是由from自增出现的值，第二个是由to产生的值。规则引擎多算一次，但为了保证正确暂时先这样写
                // trace: http://jira.taobao.ali.com/browse/TDDL-38
                retValue.add(toComparable);
                return;
            } else {
                // 枚举小于to的值,添加枚举到定义域
                retValue.add(enumedFoD);

            }
        }

    }
}
