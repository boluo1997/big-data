package com.boluo.others;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author chao
 * @datetime 2025-02-26 22:24
 * @description
 */
public class Demo01 {

    @Test
    public void func1() {
        List<String> list = Lists.newArrayList("A", "B", "C", "D");
        System.out.println(list);

        Iterator<String> iterator = list.iterator();
        iterator.hasNext();

        for (int i = 0; i < list.size(); i++) {
            System.out.print("当前i: " + i);
            String next = iterator.next();
            System.out.print("当前迭代器获取到的值: ");
            iterator.hasNext();
            System.out.println(next);
        }

    }
}
