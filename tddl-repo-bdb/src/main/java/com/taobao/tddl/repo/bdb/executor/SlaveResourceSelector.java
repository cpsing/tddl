package com.taobao.tddl.repo.bdb.executor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.taobao.tddl.common.exception.TddlException;

/**
 * 读库选择器
 * 
 * @author Whisper
 */
public class SlaveResourceSelector extends CommonResourceSelector {

    // private static final Log logger =
    // LogFactory.getLog(SlaveResourceSelector.class);

    private final Random               random = new Random();

    protected final ArrayList<Integer> rExecutorList;

    public SlaveResourceSelector(Integer executorSize, List<Integer> rWeight){
        super(executorSize);
        this.rExecutorList = new ArrayList<Integer>();
        int rweightIndex = 0;
        for (Integer exe : executorList) {
            int weight = rWeight.get(rweightIndex);
            for (int i = 0; i < weight; i++) {

                rExecutorList.add(exe);
            }
            rweightIndex++;
        }
    }

    @Override
    public Integer select(Map<Integer, String> excludeKey) throws TddlException {

        if (excludeKey == null) {
            Integer commandExecutor = rExecutorList.get(random.nextInt(rExecutorList.size()));
            return commandExecutor;
        } else {

            ArrayList<Integer> clone = removeExcludeKey(excludeKey);

            int restSize = clone.size();
            if (restSize == 0) {
                throw new TddlException("No More selections ." + excludeKey);
            }
            int index = random.nextInt(restSize);
            return clone.get(index);
        }
    }

    @Override
    public int getRetryTimes() {
        return executorList.size();
    }

}
