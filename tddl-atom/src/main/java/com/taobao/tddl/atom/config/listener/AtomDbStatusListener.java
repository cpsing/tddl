package com.taobao.tddl.atom.config.listener;

import com.taobao.tddl.atom.TAtomDbStatusEnum;

/**
 * 数据库状态变化监听器
 * 
 * @author qihao
 */
public interface AtomDbStatusListener {

    void handleData(TAtomDbStatusEnum oldStatus, TAtomDbStatusEnum newStatus);
}
