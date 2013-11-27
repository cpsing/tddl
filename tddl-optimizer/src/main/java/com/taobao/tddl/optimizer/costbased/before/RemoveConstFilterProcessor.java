package com.taobao.tddl.optimizer.costbased.before;

import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.utils.FilterUtils;

/**
 * @author Whisper
 */
public class RemoveConstFilterProcessor implements IBooleanFilterProcessor {

    public IFilter processBoolFilter(IFilter root) {
        IBooleanFilter bf = (IBooleanFilter) root;
        if (FilterUtils.isConstFilter(bf)) {
            return null;
        } else {
            return bf;
        }
    }
}
