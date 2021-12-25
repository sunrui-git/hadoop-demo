package com.sunrui.edu_sort;


public abstract class Sort<T extends Comparable<T>> {
    protected T[] array;

    public void sort(T[] array) {
        if (array == null || array.length < 2) return;
        this.array = array;
        sort();

        printInfo();
    }

    protected abstract void sort();

    /*
     * 返回值等于0，代表 array[i1] == array[i2]
     * 返回值小于0，代表 array[i1] < array[i2]
     * 返回值大于0，代表 array[i1] > array[i2]
     */
    protected int cmp(int i1, int i2) {
        return array[i1].compareTo(array[i2]);
    }

    protected int cmp(T v1, T v2) {
        return v1.compareTo(v2);
    }

    protected void swap(int i1, int i2) {
        T tmp = array[i1];
        array[i1] = array[i2];
        array[i2] = tmp;
    }

    public void printInfo() {
        System.out.println("==================");
        for (int i = 0; i < array.length; i++) {
            System.out.print(array[i] + "_");
        }
        System.out.println();
        System.out.println("================");
    }


}
