package com.taobao.tddl.repo.bdb.executor;

import java.util.ArrayList;
import java.util.Map;

public abstract class CommonResourceSelector implements ResourceSelector {

    // private static final Log logger =
    // LogFactory.getLog(CommonResourceSelector.class);

    protected volatile ArrayList<Integer> executorList;

    public CommonResourceSelector(Integer indexes){
        super();
        this.executorList = new ArrayList<Integer>(indexes);
        for (int i = 0; i < indexes; i++) {
            executorList.add(i);
        }
    }

    @SuppressWarnings("unchecked")
    protected ArrayList<Integer> removeExcludeKey(Map<Integer, String> excludeKey) {
        ArrayList<Integer> clone = (ArrayList<Integer>) executorList.clone();
        for (Integer key : excludeKey.keySet()) {
            while (true) {
                boolean succ = clone.remove(key);
                if (!succ) {
                    break;
                }
            }
        }
        return clone;
    }

    @Override
    public int getRetryTimes() {
        return executorList.size();
    }

}
