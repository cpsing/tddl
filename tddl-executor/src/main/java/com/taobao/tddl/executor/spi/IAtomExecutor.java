package com.taobao.tddl.executor.spi;

import com.taobao.tddl.common.model.Atom;
import com.taobao.tddl.executor.IExecutor;

public interface IAtomExecutor extends IExecutor {

    Atom getAtomInfo();
}
