package com.taobao.tddl.repo.bdb.executor;

import java.util.Map;

import com.taobao.tddl.common.exception.TddlException;

/**
 * 选择一个被选择的对象，然后进行某个操作。
 * 
 * @author whisper
 */
public interface ResourceSelector {

    Integer select(Map<Integer, String> excludeKey) throws TddlException;

    public int getRetryTimes();
}
