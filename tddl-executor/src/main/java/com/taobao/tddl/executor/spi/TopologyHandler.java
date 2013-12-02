package com.taobao.tddl.executor.spi;

import java.util.HashMap;
import java.util.Map;

/**
 * group以及其对应的执行器
 * 
 * @author mengshi.sunmengshi 2013-11-27 下午4:00:33
 * @since 5.1.0
 */
public class TopologyHandler {

    private final Map<String/* group key */, RemotingExecutor> executorMap = new HashMap<String, RemotingExecutor>();

    public Map<String, RemotingExecutor> getExecutorMap() {
        return executorMap;
    }

    public RemotingExecutor putOne(String groupKey, RemotingExecutor remotingExecutor) {
        return putOne(groupKey, remotingExecutor, true);
    }

    public RemotingExecutor putOne(String groupKey, RemotingExecutor remotingExecutor, boolean singleton) {
        if (singleton && executorMap.containsKey(groupKey)) {
            throw new IllegalArgumentException("group key is already exists . group key : " + groupKey + " . map "
                                               + executorMap);
        }
        return executorMap.put(groupKey, remotingExecutor);
    }

    public RemotingExecutor get(Object key) {
        return executorMap.get(key);
    }

    @Override
    public String toString() {
        return "TopologyHandler [executorMap=" + executorMap + "]";
    }

}
