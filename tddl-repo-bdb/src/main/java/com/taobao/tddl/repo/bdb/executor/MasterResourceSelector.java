package com.taobao.tddl.repo.bdb.executor;

import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.common.exception.TddlException;

public class MasterResourceSelector extends CommonResourceSelector {

    private static final Log    logger         = LogFactory.getLog(MasterResourceSelector.class);
    // private final AtomicInteger masterIndex = new AtomicInteger(0);

    private final AtomicInteger masterExecutor = new AtomicInteger(0);
    private final Random        random         = new Random();

    public MasterResourceSelector(Integer index){
        super(index);
        if (index == 0) {
            throw new IllegalArgumentException("executor list size is 0");
        }

    }

    @Override
    public Integer select(Map<Integer, String> excludeKey) throws TddlException {

        if (excludeKey == null) {
            return masterExecutor.get();
        } else {
            Integer temp = masterExecutor.get();
            if (!excludeKey.containsKey(temp)) {
                logger.warn("not contains key :" + excludeKey + " .temp key is " + temp);
                return temp;
            } else {
                ArrayList<Integer> clone = removeExcludeKey(excludeKey);

                int restSize = clone.size();
                if (restSize == 0) {
                    throw new TddlException("No More selections ." + excludeKey);
                }
                // 这里存在随机选择错误，导致后续选择也会错误的问题，但这依赖excludeKey来协调。
                int index = random.nextInt(restSize);
                masterExecutor.set(clone.get(index));
                return masterExecutor.get();

            }
        }
    }

    @Override
    public int getRetryTimes() {
        return executorList.size();
    }

    @Override
    public String toString() {
        return "MasterResourceSelector [masterExecutor=" + masterExecutor + "]";
    }

    public AtomicInteger getMasterExecutor() {
        return masterExecutor;
    }

}
