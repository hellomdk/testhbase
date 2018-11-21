package com.bigdata;

import static org.junit.Assert.*;

public class HbaseUtilsTest {

    public static void main(String[] args) {
        HbaseUtils hu = new HbaseUtils();
        for (String table :hu.listTables())
        {
            System.out.println(table);
        }
    }

}