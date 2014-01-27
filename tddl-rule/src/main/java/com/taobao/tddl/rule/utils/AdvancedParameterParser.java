package com.taobao.tddl.rule.utils;

import java.util.Calendar;

import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.rule.model.AdvancedParameter;
import com.taobao.tddl.rule.model.AdvancedParameter.AtomIncreaseType;
import com.taobao.tddl.rule.model.AdvancedParameter.Range;
import com.taobao.tddl.rule.model.DateEnumerationParameter;

/**
 * {@linkplain AdvancedParameter}解析器
 * 
 * @author jianghang 2013-10-29 下午5:18:27
 * @since 5.0.0
 */
public class AdvancedParameterParser {

    public static final String PARAM_SEGMENT_SPLITOR           = ",";
    public static final char   NEED_APPEAR_SYMBOL              = '?';
    public static final String INCREASE_TYPE_SPLITOR           = "_";
    public static final String RANGE_SEGMENT_SPLITOR           = "|";
    public static final String RANGE_SEGMENT_START_END_SPLITOR = "_";

    /**
     * @param paramToken 定义变量的分表片段，形式类似 #gmt_create?,1_month,-12_12#
     * #id,1_number,1024# #name,1_string,a_z# #id,1_number,0_1024|1m_1g#
     * @param completeConfig 如果为true,那么paramToken必须满足逗号分隔的3段形式
     * 如果为false,那么paramToken可以只配置分表或者分表键 2.3.x－2.4.3的老规则配置该参数为false
     * 2.4.4后支持的新规则配置该参数为true;
     */
    public static AdvancedParameter getAdvancedParamByParamTokenNew(String paramToken, boolean completeConfig) {
        String key;
        boolean[] needAppear = new boolean[1];

        AtomIncreaseType atomicIncreateType = null;
        Comparable<?> atomicIncreateValue = null;

        Range[] rangeObjectArray = null;
        Integer cumulativeTimes = null;

        String[] paramTokens = TStringUtil.split(paramToken, PARAM_SEGMENT_SPLITOR);
        switch (paramTokens.length) {
            case 1:
                if (completeConfig) {
                    throw new IllegalArgumentException("规则必须配置完全，格式如下:#id,1_number,1024#");
                }
                key = parseKeyPart(paramTokens[0], needAppear);
                break;
            case 2:
                // 若只有两个，自增类型默认为number，自增值默认为1； 其他同case 3
                key = parseKeyPart(paramTokens[0], needAppear);

                atomicIncreateType = AtomIncreaseType.NUMBER;
                atomicIncreateValue = 1;

                try {
                    rangeObjectArray = parseRangeArray(paramTokens[1]);
                    cumulativeTimes = getCumulativeTimes(rangeObjectArray[0]);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("输入的参数不为Integer类型,参数为:" + paramToken, e);
                } catch (Exception e) {
                    throw new IllegalArgumentException(e);
                }

                break;
            case 3:
                key = parseKeyPart(paramTokens[0], needAppear);
                try {

                    atomicIncreateType = getIncreaseType(paramTokens[1]);
                    atomicIncreateValue = getAtomicIncreaseValue(paramTokens[1], atomicIncreateType);
                    rangeObjectArray = parseRangeArray(paramTokens[2]);
                    // 长度为三必定有范围定义，否则直接抛错
                    // 如果范围有多段("|"分割)，那么以第一段的跨度为标准
                    cumulativeTimes = getCumulativeTimes(rangeObjectArray[0]);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("输入的参数不为Integer类型,参数为:" + paramToken, e);
                } catch (Exception e) {
                    throw new IllegalArgumentException(e);
                }
                break;
            default:
                throw new IllegalArgumentException("错误的参数个数，必须为1个或者3个，3个的时候为允许使用" + "枚举时的数据");
        }

        return new AdvancedParameter(key,
            atomicIncreateValue,
            cumulativeTimes,
            needAppear[0],
            atomicIncreateType,
            rangeObjectArray);
    }

    /**
     * ColumnName?表示可选
     * 
     * @param keyPart 不可能传入null
     */
    private static String parseKeyPart(String keyPart, boolean[] needAppear) {
        String key;
        keyPart = keyPart.trim();
        int endIndex = keyPart.length() - 1;
        if (keyPart.charAt(endIndex) == NEED_APPEAR_SYMBOL) {
            needAppear[0] = true;
            key = keyPart.substring(0, endIndex);
        } else {
            needAppear[0] = false;
            key = keyPart;
        }
        return key;
    }

    private static AtomIncreaseType getIncreaseType(String paramTokenStr) {
        String[] increase = TStringUtil.split(paramTokenStr.trim(), INCREASE_TYPE_SPLITOR);
        if (increase.length == 1) {
            return AtomIncreaseType.NUMBER;
        } else if (increase.length == 2) {
            return AtomIncreaseType.valueOf(increase[1].toUpperCase());
        } else {
            throw new IllegalArgumentException("自增配置定义错误:" + paramTokenStr);
        }
    }

    private static Comparable<?> getAtomicIncreaseValue(String paramTokenStr, AtomIncreaseType type) {
        String[] increase = TStringUtil.split(paramTokenStr.trim(), INCREASE_TYPE_SPLITOR);
        // 如果长度为1,那么默认为数字类型
        if (increase.length == 1) {
            return Integer.valueOf(increase[0]);
        } else if (increase.length == 2) {
            switch (type) {
                case NUMBER:
                    return Integer.valueOf(increase[0]);
                case DATE:
                    return new DateEnumerationParameter(Integer.valueOf(increase[0]), Calendar.DATE);
                case MONTH:
                    return new DateEnumerationParameter(Integer.valueOf(increase[0]), Calendar.MONTH);
                case YEAR:
                    return new DateEnumerationParameter(Integer.valueOf(increase[0]), Calendar.YEAR);
                case HOUR:
                    return new DateEnumerationParameter(Integer.valueOf(increase[0]), Calendar.HOUR_OF_DAY);
                default:
                    throw new IllegalArgumentException("不支持的自增类型：" + type);
            }
        } else {
            throw new IllegalArgumentException("自增配置定义错误:" + paramTokenStr);
        }
    }

    private static Range[] parseRangeArray(String paramTokenStr) {
        String[] ranges = TStringUtil.split(paramTokenStr, RANGE_SEGMENT_SPLITOR);
        Range[] rangeObjArray = new Range[ranges.length];

        for (int i = 0; i < ranges.length; i++) {
            String range = ranges[i].trim();
            String[] startEnd = TStringUtil.split(range, RANGE_SEGMENT_START_END_SPLITOR);
            if (startEnd.length == 1) {
                if (i == 0) {
                    rangeObjArray[i] = new Range(Integer.valueOf(0), Integer.valueOf(startEnd[0]));
                } else {
                    rangeObjArray[i] = new Range(fromReadableInt(startEnd[0]), fromReadableInt(startEnd[0]));
                }
            } else if (startEnd.length == 2) {
                rangeObjArray[i] = new Range(fromReadableInt(startEnd[0]), fromReadableInt(startEnd[1]));
            } else {
                throw new IllegalArgumentException("范围定义错误," + paramTokenStr);
            }
        }
        return rangeObjArray;
    }

    /**
     * 1m = 1,000,000; 2M = 2,000,000 1g = 1,000,000,000 3G = 3,000,000,000
     */
    private static int fromReadableInt(String readableInt) {
        char c = readableInt.charAt(readableInt.length() - 1);
        if (c == 'm' || c == 'M') {
            return Integer.valueOf(readableInt.substring(0, readableInt.length() - 1)) * 1000000;
        } else if (c == 'g' || c == 'G') {
            return Integer.valueOf(readableInt.substring(0, readableInt.length() - 1)) * 1000000000;
        } else {
            return Integer.valueOf(readableInt);
        }
    }

    private static Integer getCumulativeTimes(Range ro) {
        return ro.end - ro.start;
    }
}
