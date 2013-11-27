package com.taobao.tddl.optimizer.costbased.before;

import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;
import com.taobao.tddl.optimizer.utils.FilterUtils;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

public class TypeConvertProcessor implements IBooleanFilterProcessor {

    @Override
    public IFilter processBoolFilter(IFilter root) {
        IBooleanFilter bf = (IBooleanFilter) root;
        // 如果是 1 = id情况
        if (FilterUtils.isConstValue(bf.getColumn()) && !FilterUtils.isConstValue(bf.getValue())) {
            DATA_TYPE type = null;
            if (bf.getValue() instanceof IColumn) {
                type = ((IColumn) bf.getValue()).getDataType();
            }

            if (bf.getValue() instanceof IFunction) {
                type = ((IFunction) bf.getValue()).getDataType();
            }

            bf.setColumn(OptimizerUtils.convertType(bf.getColumn(), type));
        }

        // 如果是 id = 1情况
        if (FilterUtils.isConstValue(bf.getValue()) && !FilterUtils.isConstValue(bf.getColumn())) {
            DATA_TYPE type = null;
            if (bf.getColumn() instanceof IColumn) {
                type = ((IColumn) bf.getColumn()).getDataType();
            }

            if (bf.getColumn() instanceof IFunction) {
                type = ((IFunction) bf.getColumn()).getDataType();
            }

            bf.setValue((Comparable) OptimizerUtils.convertType(bf.getValue(), type));
        }

        // 如果是id in (xx)
        if (bf.getValues() != null) {
            for (int i = 0; i < bf.getValues().size(); i++) {
                bf.getValues().set(i,
                    (Comparable) OptimizerUtils.convertType(bf.getValues().get(i),
                        ((ISelectable) bf.getColumn()).getDataType()));
            }
        }

        // 如果是 1=1
        if (FilterUtils.isConstFilter(bf)) {
            return null;
        } else {
            return bf;
        }

    }

}
