package com.sunrui.group;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author sunrui
 * @description 默认的分区规则，区内有序
 * @date 2021/11/10
 */
public class OrderBean implements WritableComparable<OrderBean> {

    private String orderId;//订单id
    private Double price;//金额


    public OrderBean(String orderId, Double price) {
        this.orderId = orderId;
        this.price = price;
    }

    public OrderBean() {
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }
    @Override
    public int compareTo(OrderBean o) {
        return -this.price.compareTo(o.getPrice());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readUTF();
        this.price = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return orderId + '\t' + price;
    }
}
