package com.taobao.tddl.rule.virtualnode;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.rule.enumerator.handler.CloseIntervalFieldsEnumeratorHandler;
import com.taobao.tddl.rule.enumerator.handler.IntegerPartDiscontinousRangeEnumerator;
import com.taobao.tddl.rule.model.sqljep.Comparative;

/**
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 1.0
 * @since 1.6
 * @date 2011-8-20 02:58:34
 */
public class WrappedLogic extends AbstractLifecycle {

    private static final String                  SLOT_PIECE_SPLIT   = ",";
    private static final String                  RANGE_SUFFIX_SPLIT = "-";
    private CloseIntervalFieldsEnumeratorHandler enumerator         = new IntegerPartDiscontinousRangeEnumerator();
    protected String                             valuePrefix;                                                      // 无getter/setter
    protected String                             valueSuffix;                                                      // 无getter/setter
    protected int                                valueAlignLen      = 0;                                           // 无getter/setter
    protected String                             tableSlotKeyFormat = null;

    public void setTableSlotKeyFormat(String tableSlotKeyFormat) {
        if (tableSlotKeyFormat == null) {
            return;
        }

        this.tableSlotKeyFormat = tableSlotKeyFormat;

        int index0 = tableSlotKeyFormat.indexOf('{');
        if (index0 == -1) {
            this.valuePrefix = tableSlotKeyFormat;
            return;
        }
        int index1 = tableSlotKeyFormat.indexOf('}', index0);
        if (index1 == -1) {
            this.valuePrefix = tableSlotKeyFormat;
            return;
        }
        this.valuePrefix = tableSlotKeyFormat.substring(0, index0);
        this.valueSuffix = tableSlotKeyFormat.substring(index1 + 1);
        this.valueAlignLen = index1 - index0 - 1;// {0000}中0的个数
    }

    protected String wrapValue(String value) {
        StringBuilder sb = new StringBuilder();
        if (valuePrefix != null) {
            sb.append(valuePrefix);
        }

        if (valueAlignLen > 1) {
            int k = valueAlignLen - value.length();
            for (int i = 0; i < k; i++) {
                sb.append("0");
            }
        }
        sb.append(value);
        if (valueSuffix != null) {
            sb.append(valueSuffix);
        }
        return sb.toString();
    }

    /**
     * <pre>
     * 参数oriMap的value格式为 <b>0,1,2-6</b> 0,1表示2个槽,'-'表示一个范围
     * 
     * 此函数中将范围枚举成一个个槽,并将槽变为key,原本的key变为value
     * 
     * example 1:key为 1 value为 1,2,3-6
     * 
     * 返回结果为 1->1,2->1,3->1,4->1,5->1,6->1
     * 
     * example 2:key为db_group_1 value为1,2 db_group_2 value为3,4-6
     * 返回结果为 1->db_group_1,2->db_group_1
     * 3->db_group_2,4->db_group_2,5->db_group_3,2->db_group_2
     * 
     * <b>
     * 暂时不支持任何形式的value格式化.即_0000,0001之类的字符串,只接受
     * 数学形式上的integer,long
     * 
     * 后续改进
     * </b>
     * </pre>
     * 
     * @param tableMap
     * @return
     */
    protected Map<String, String> extraReverseMap(Map<String, String> oriMap) {
        ConcurrentHashMap<String, String> slotMap = new ConcurrentHashMap<String, String>();
        for (Map.Entry<String, String> entry : oriMap.entrySet()) {
            String[] pieces = entry.getValue().trim().split(SLOT_PIECE_SPLIT);
            for (String piece : pieces) {
                String[] range = piece.trim().split(RANGE_SUFFIX_SPLIT);
                if (range.length == 2) {
                    Comparative start = new Comparative(Comparative.GreaterThanOrEqual, Integer.valueOf(range[0]));
                    Comparative end = new Comparative(Comparative.LessThanOrEqual, Integer.valueOf(range[1]));
                    int cumulativeTimes = Integer.valueOf(range[1]) - Integer.valueOf(range[0]);
                    Set<Object> result = new HashSet<Object>();
                    enumerator.mergeFeildOfDefinitionInCloseInterval(start, end, result, cumulativeTimes, 1);
                    for (Object v : result) {
                        slotMap.put(String.valueOf(v), entry.getKey());
                    }
                } else if (range.length == 1) {
                    slotMap.put(piece, entry.getKey());
                } else {
                    throw new IllegalArgumentException("slot config error,slot piece:" + piece);
                }
            }
        }
        return slotMap;
    }
}
