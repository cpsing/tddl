package com.taobao.tddl.executor.common;

import java.util.HashMap;
import java.util.Map;

import com.taobao.tddl.executor.spi.IGroupExecutor;

/**
 * group以及其对应的执行器
 * 
 * @author mengshi.sunmengshi 2013-11-27 下午4:00:33
 * @since 5.1.0
 */
public class TopologyHandler {

    private final Map<String/* group key */, IGroupExecutor> executorMap = new HashMap<String, IGroupExecutor>();

    public Map<String, IGroupExecutor> getExecutorMap() {
        return executorMap;
    }

    public IGroupExecutor putOne(String groupKey, IGroupExecutor groupExecutor) {
        return putOne(groupKey, groupExecutor, true);
    }

    public IGroupExecutor putOne(String groupKey, IGroupExecutor groupExecutor, boolean singleton) {
        if (singleton && executorMap.containsKey(groupKey)) {
            throw new IllegalArgumentException("group key is already exists . group key : " + groupKey + " . map "
                                               + executorMap);
        }
        return executorMap.put(groupKey, groupExecutor);
    }

    public IGroupExecutor get(Object key) {
        return executorMap.get(key);
    }

    @Override
    public String toString() {
        return "TopologyHandler [executorMap=" + executorMap + "]";
    }

}
