package com.taobao.tddl.optimizer.costbased.before;

import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.utils.FilterUtils;

public class FilterExchangerProcessor implements IBooleanFilterProcessor {

    public IFilter processBoolFilter(IFilter root) {
        IBooleanFilter bf = (IBooleanFilter) root;
        if (!FilterUtils.isConstValue(bf.getValue())) {
            if (FilterUtils.isConstValue(bf.getColumn())) {
                Comparable val = bf.getColumn();
                bf.setColumn(bf.getValue());
                bf.setValue(val);
            } else {
                throw new IllegalArgumentException("sub query like column in/= (subquery) has not supported yet, using join instant .");
            }
        }
        return bf;
    }

}
