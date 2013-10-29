package com.taobao.tddl.rule.utils;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.rule.model.AdvancedParameter;
import com.taobao.tddl.rule.model.AdvancedParameter.AtomIncreaseType;
import com.taobao.tddl.rule.model.AdvancedParameter.Range;

public class AdvancedParameterParserTest {

    @Test
    public void test_正常() {
        String param = "id,1_number,1024";
        AdvancedParameter result = AdvancedParameterParser.getAdvancedParamByParamTokenNew(param, false);
        testResult(result, AtomIncreaseType.NUMBER, 1, 1024);

        param = "id,1024";
        result = AdvancedParameterParser.getAdvancedParamByParamTokenNew(param, false);
        testResult(result, AtomIncreaseType.NUMBER, 1, 1024);
    }

    @Test
    public void test_范围() {
        String param = "id,1_number,0_1024|1m_1g";
        AdvancedParameter result = AdvancedParameterParser.getAdvancedParamByParamTokenNew(param, false);
        testResult(result,
            AtomIncreaseType.NUMBER,
            new AdvancedParameter.Range[] { getRange(0, 1024), getRange(1 * 1000000, 1 * 1000000000) },
            1);

        param = "id,0_1024|1m_1g";
        result = AdvancedParameterParser.getAdvancedParamByParamTokenNew(param, false);
        testResult(result,
            AtomIncreaseType.NUMBER,
            new AdvancedParameter.Range[] { getRange(0, 1024), getRange(1 * 1000000, 1 * 1000000000) },
            1);
    }

    private void testResult(AdvancedParameter result, AtomIncreaseType type, Comparable atomicIncreateValue,
                            Integer cumulativeTimes) {
        Assert.assertEquals(result.atomicIncreateType, type);
        Assert.assertEquals(result.atomicIncreateValue, atomicIncreateValue);
        Assert.assertEquals(result.cumulativeTimes, cumulativeTimes);
    }

    private void testResult(AdvancedParameter result, AtomIncreaseType type, Range[] rangeValue,
                            Integer atomicIncreateValue) {
        Assert.assertEquals(result.atomicIncreateType, type);
        Assert.assertEquals(result.atomicIncreateValue, atomicIncreateValue);
        int i = 0;
        for (Range range : result.rangeArray) {
            Assert.assertEquals(range.start, rangeValue[i].start);
            Assert.assertEquals(range.end, rangeValue[i].end);
            i++;
        }
    }

    private AdvancedParameter.Range getRange(int start, int end) {
        return new AdvancedParameter.Range(start, end);
    }

}
