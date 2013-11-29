package com.taobao.tddl.optimizer.costbased;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.taobao.tddl.optimizer.utils.PermutationGenerator;

public class PermutationGeneratorTest {

    @Test
    public void testNext() {
        List<Integer> elements = new ArrayList<Integer>();

        elements.add(1);
        elements.add(2);
        elements.add(3);

        PermutationGenerator instance = new PermutationGenerator(elements);

        while (instance.hasNext()) {
            List result = instance.next();
            System.out.println(result);
        }

    }
}
