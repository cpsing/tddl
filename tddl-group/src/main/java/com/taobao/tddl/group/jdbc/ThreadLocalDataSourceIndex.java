package com.taobao.tddl.group.jdbc;

import com.taobao.tddl.common.model.ThreadLocalString;
import com.taobao.tddl.common.utils.thread.ThreadLocalMap;
import com.taobao.tddl.group.config.GroupIndex;
import com.taobao.tddl.group.dbselector.DBSelector;

/**
 * @author yangzhu
 */
public class ThreadLocalDataSourceIndex {

    public static boolean existsIndex() {
        return getIndexAsObject() != null;
    }

    public static Integer getIndexAsObject() {
        Integer indexObject = null;
        try {
            indexObject = (Integer) ThreadLocalMap.get(ThreadLocalString.DATASOURCE_INDEX);
            if (indexObject == null) {
                return null;
            }
            return indexObject;
        } catch (Exception e) {
            throw new IllegalArgumentException(msg(indexObject));
        }
    }

    public static GroupIndex getIndex() {
        Integer indexObject = null;
        try {
            indexObject = (Integer) ThreadLocalMap.get(ThreadLocalString.DATASOURCE_INDEX);
            // 不存在索引时返回-1，这样调用者只要知道返回值是-1就会认为业务层没有设置过索引
            if (indexObject == null) {
                return new GroupIndex(DBSelector.NOT_EXIST_USER_SPECIFIED_INDEX, false);
            }

            int index = indexObject.intValue();
            // 如果业务层已设置了索引，此时索引不能为负值
            if (index < 0) {
                throw new IllegalArgumentException(msg(indexObject));
            }

            boolean failRetryFlag = ThreadLocalDataSourceIndex.getFailRetryFlag();
            return new GroupIndex(index, failRetryFlag);
        } catch (Exception e) {
            throw new IllegalArgumentException(msg(indexObject));
        }
    }

    private static boolean getFailRetryFlag() {
        Boolean failOver = (Boolean) ThreadLocalMap.get(ThreadLocalString.RETRY_IF_SET_DS_INDEX);
        if (failOver == null) {
            return false;
        } else {
            return failOver;
        }
    }

    public static void clearIndex() {
        ThreadLocalMap.remove(ThreadLocalString.DATASOURCE_INDEX);
        ThreadLocalMap.remove(ThreadLocalString.RETRY_IF_SET_DS_INDEX);
    }

    private static String msg(Integer indexObject) {
        return indexObject + " 不是一个有效的数据源索引，索引只能是大于0的数字";
    }
}
