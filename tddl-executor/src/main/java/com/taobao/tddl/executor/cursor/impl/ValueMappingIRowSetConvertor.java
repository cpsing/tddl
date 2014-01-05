package com.taobao.tddl.executor.cursor.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.executor.rowset.IRowSet;

public class ValueMappingIRowSetConvertor {

    protected ICursorMeta                                                returnCursorMeta;

    /**
     * 在每次切换了一个cursor以后，需要取第一个记录.这个标记的作用就是告诉下一次的循环：
     * 是一个新的cursor了，需要拿第一个记录出来和看看是否需要做匹配。
     */
    protected boolean                                                    firstRowInNewCursor = true;
    /**
     * 用来统一所有MergeCursor中的index, 以第一个为模板。
     * 如果和第一个的index不匹配，那么valueMappingMap就会有一些对应关系，
     * 存放了第一个cursor中的index值和后续其他cursor的index值之间的对应关系。
     */
    private Map<Integer /* 返回列中的index位置 */, Integer /* 实际数据中的index位置 */> valueMappingMap     = null;

    public IRowSet wrapValueMappingIRowSetIfNeed(IRowSet firstIRowSet) {
        if (!firstRowInNewCursor) {
            return wrapValueMappingIRowSetIfValueMappingIsNotNull(firstIRowSet);
        }
        firstRowInNewCursor = false;
        if (this.returnCursorMeta == null) {
            returnCursorMeta = firstIRowSet.getParentCursorMeta();
        }

        Iterator<ColMetaAndIndex> returnMetaIterator = returnCursorMeta.indexIterator();
        while (returnMetaIterator.hasNext()) {
            ColMetaAndIndex colMetaAndIndex = returnMetaIterator.next();
            Integer indexInCursor = firstIRowSet.getParentCursorMeta().getIndex(colMetaAndIndex.getTable(),
                colMetaAndIndex.getName());
            if (indexInCursor == null) {// return中有的数据但当前cursor中没有
                throw new IllegalArgumentException("can't " + "find column in sub cursor : "
                                                   + colMetaAndIndex.getTable() + "." + colMetaAndIndex.getName());
            }
            valueMappingMap = putIntoValueMappingIfNotEquals(valueMappingMap, colMetaAndIndex, indexInCursor);
        }
        return wrapValueMappingIRowSetIfValueMappingIsNotNull(firstIRowSet);
    }

    private IRowSet wrapValueMappingIRowSetIfValueMappingIsNotNull(IRowSet firstIRowSet) {
        if (valueMappingMap != null) {
            return ValueMappingCursor.wrap(returnCursorMeta, firstIRowSet, valueMappingMap);
        } else {
            return firstIRowSet;
        }
    }

    private Map<Integer /* 返回列中的index位置 */, Integer /* 实际数据中的index位置 */> putIntoValueMappingIfNotEquals(Map<Integer, Integer> valueMappingMap,
                                                                                                        ColMetaAndIndex colMetaAndIndex,
                                                                                                        Integer indexInCursor) {
        if (!indexInCursor.equals(colMetaAndIndex.getIndex())) {
            if (valueMappingMap == null) {
                valueMappingMap = new HashMap<Integer, Integer>(8);
            }
            valueMappingMap.put(colMetaAndIndex.getIndex(), indexInCursor);
        }
        return valueMappingMap;
    }

    public void reset() {
        valueMappingMap = null;
        firstRowInNewCursor = true;
    }
}
