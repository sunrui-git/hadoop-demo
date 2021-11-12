package com.sunrui.group;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.util.Comparator;

/**
 * @author sunrui
 * @description
 * @date 2021/11/10
 */
public class CustomGroupingComparator extends WritableComparator {

    //注册自定义的GroupingComparator接受OrderBean对象
    public CustomGroupingComparator(){
        super(OrderBean.class,true);
    }

    //重写其中的compare方法，通过这个方法来让mr接受orderid相等则两个对象相等的规则，key相等
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;

        return aBean.getOrderId().compareTo(bBean.getOrderId());
    }
}
