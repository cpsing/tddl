package com.taobao.tddl.optimizer.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

/**
 * 用来生成一个序列的全排列 非递归
 * 
 * @author Dreamond
 */
public final class PermutationGenerator {

    private List           elements;
    private Stack<List>    currentState     = new Stack();
    private Stack<Integer> currentUsedStack = new Stack();
    private List           permutation      = new ArrayList();

    public PermutationGenerator(List elements){
        this.elements = elements;
        this.reset();
    }

    public void reset() {
        this.currentState.clear();

        List rest = new ArrayList(elements);
        currentState.push(rest);
        currentUsedStack.push(-1);
    }

    public List next() {
        boolean flag = true;
        while (permutation.size() < this.elements.size() || flag) {
            flag = false;

            List<?> rest = new ArrayList(currentState.peek());
            Integer currentUsed = currentUsedStack.peek();
            if (currentUsed != rest.size() - 1) {
                if (!permutation.isEmpty() && currentUsed > -1
                    && permutation.get(permutation.size() - 1).equals(rest.get(currentUsed))) {
                    permutation.remove(permutation.size() - 1);
                } else {
                    currentUsed++;
                    permutation.add(rest.get(currentUsed));
                    rest.remove((int) currentUsed);

                    currentState.push(rest);
                    currentUsedStack.pop();
                    currentUsedStack.push(currentUsed);
                    currentUsedStack.push(-1);
                }
            } else {
                this.currentState.pop();
                currentUsedStack.pop();

                permutation.remove(permutation.size() - 1);
                flag = true;
            }
        }

        return new ArrayList(permutation);
    }

    public boolean hasNext() {
        if (elements.size() != this.permutation.size()) {
            return true;
        }
        for (int i = 0; i < this.elements.size(); i++) {
            if (!elements.get(i).equals(this.permutation.get(this.permutation.size() - 1 - i))) {
                return true;
            }
        }
        return false;
    }

}
