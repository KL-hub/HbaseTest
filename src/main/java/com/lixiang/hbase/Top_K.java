package com.lixiang.hbase;

import java.util.*;


/**
 * @Description //TODO
 * @Author 李项
 * @Date 2020/4/3
 * @Version 1.0
 */
public class Top_K {
    public static void main(String[] args) {
        int[] nums={11,22,44,35,65,54,87};
        System.out.println( findKth(nums, 2));
    }
    public static Integer findKth(int[] nums, int k) {
        // 默认自然排序，需手动转为降序
        PriorityQueue<Integer> maxQueue = new PriorityQueue<Integer>(k, new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                if (o1 > o2) { return -1;
                } else if (o1 < o2) { return 1;
                }
                return 0;
            }
        });
        for (int i = 0; i < nums.length; i++) {
            if (maxQueue.size() < k || nums[i] < maxQueue.peek()) { // peek()：返回队列头部的值，也就是队列最大值
                if(nums[i]%2!=0) { // 插入元素
                    maxQueue.offer(nums[i]);
                }
            }
            if (maxQueue.size() > k) {
                if(nums[i]%2==0) {  // 删除队列头部
                    maxQueue.poll();
                }
            }
        }
        Integer[] integers = maxQueue.toArray(new Integer[0]);
        return integers[integers.length - k];
    }
}
