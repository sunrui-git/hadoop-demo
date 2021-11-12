package com.sunrui.comment.step3;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author sunrui
 * @description
 * @date 2021/11/12
 */
public class CommentBean implements WritableComparable<CommentBean> {
    private String orderId;
    private String comment;
    private String commentExt;
    private int goodsNum;
    private String phoneNum;
    private String userName;
    private String address;
    private int commentStatus;
    private String commentTime;

    @Override
    public String toString() {
        return orderId+"\t"+comment+"\t"+commentExt+"\t"+goodsNum+"\t"+phoneNum+"\t"+userName+"\t"+address+"\t"+commentStatus+"\t"+commentTime;
    }

    public CommentBean() {
    }

    public CommentBean(String orderId, String comment, String commentExt, int goodsNum, String phoneNum, String userName, String address, int commentStatus, String commentTime) {
        this.orderId = orderId;
        this.comment = comment;
        this.commentExt = commentExt;
        this.goodsNum = goodsNum;
        this.phoneNum = phoneNum;
        this.userName = userName;
        this.address = address;
        this.commentStatus = commentStatus;
        this.commentTime = commentTime;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getCommentExt() {
        return commentExt;
    }

    public void setCommentExt(String commentExt) {
        this.commentExt = commentExt;
    }

    public int getGoodsNum() {
        return goodsNum;
    }

    public void setGoodsNum(int goodsNum) {
        this.goodsNum = goodsNum;
    }

    public String getPhoneNum() {
        return phoneNum;
    }

    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getCommentStatus() {
        return commentStatus;
    }

    public void setCommentStatus(int commentStatus) {
        this.commentStatus = commentStatus;
    }

    public String getCommentTime() {
        return commentTime;
    }

    public void setCommentTime(String commentTime) {
        this.commentTime = commentTime;
    }

    @Override
    public int compareTo(CommentBean o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeUTF(comment);
        dataOutput.writeUTF(commentExt);
        dataOutput.writeInt(goodsNum);
        dataOutput.writeUTF(phoneNum);
        dataOutput.writeUTF(userName);
        dataOutput.writeUTF(address);
        dataOutput.writeInt(commentStatus);
        dataOutput.writeUTF(commentTime);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readUTF();
        this.comment = dataInput.readUTF();
        this.commentExt = dataInput.readUTF();
        this.goodsNum = dataInput.readInt();
        this.phoneNum = dataInput.readUTF();
        this.userName = dataInput.readUTF();
        this.address = dataInput.readUTF();
        this.commentStatus = dataInput.readInt();
        this.commentTime = dataInput.readUTF();
    }
}
