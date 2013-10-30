package com.taobao.tddl.atom.config.listener;

import com.taobao.tddl.atom.AtomDbStatusEnum;

/**
 * 数据库状态变化监听器
 * 
 * @author qihao
 */
public interface TAtomDbStatusListener {

    void handleData(AtomDbStatusEnum oldStatus, AtomDbStatusEnum newStatus);
}
