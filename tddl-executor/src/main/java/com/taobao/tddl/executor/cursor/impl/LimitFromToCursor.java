package com.taobao.tddl.executor.cursor.impl;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.cursor.ILimitFromToCursor;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.SchematicCursor;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.utils.ExecUtils;

/**
 * 用于做limit操作
 * 
 * @author mengshi.sunmengshi 2013-12-3 上午10:56:23
 * @since 5.0.0
 */
public class LimitFromToCursor extends SchematicCursor implements ILimitFromToCursor {

    public LimitFromToCursor(ISchematicCursor cursor, Long limitFrom, Long offset){
        super(cursor, null, cursor.getOrderBy());
        this.limitFrom = limitFrom;
        this.offset = offset;
    }

    private Long limitFrom = null;
    /**
     * fixed by shenxun: 实际含义是偏移量。非limit to的概念，所以改了名字
     */
    private Long offset    = null;
    private int  count     = 0;

    @Override
    protected void init() throws TddlException {
        if (inited) {
            return;
        }
        if (limitFrom != null && limitFrom != 0l) {
            long n = limitFrom;
            while (n != 0 && n-- > 0) {
                GeneralUtil.checkInterrupted();
                if (cursor.next() == null) {
                    break;
                }
            }
        }
        super.init();
    }

    @Override
    public IRowSet next() throws TddlException {
        init();
        count++;
        if (offset != null) {
            if (offset == 0l) {// limit To == 0 表示没有限制。
                return super.next();
            } else if (offset != null && count > offset) {// 表示当前已经取出的值>limit
                                                          // to限定
                return null;
            } else {// 表示当前已经取出的值<limit to限定。
                return super.next();
            }
        } else {
            return super.next();
        }

    }

    @Override
    public String toString() {
        return toStringWithInden(0);
    }

    @Override
    public String toStringWithInden(int inden) {
        StringBuilder sb = new StringBuilder();
        String tab = GeneralUtil.getTab(inden);
        sb.append(tab).append("【Limit cursor . from : ").append(limitFrom).append(" to :").append(offset).append("\n");
        ExecUtils.printOrderBy(orderBys, inden, sb);
        sb.append(super.toStringWithInden(inden));
        return sb.toString();
    }

    @Override
    public void beforeFirst() {
        inited = false;
        count = 0;
    }

}
