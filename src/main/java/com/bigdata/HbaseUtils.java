package com.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;
import java.util.ArrayList;

public class HbaseUtils {

    private Configuration config;
    private Connection conn;
    private HBaseAdmin admin;

    /**
     * 构造方法
     */
    public HbaseUtils() {
        config = HBaseConfiguration.create();
        try {
            conn = ConnectionFactory.createConnection(config);
            admin = (HBaseAdmin) conn.getAdmin();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 关闭连接
     * @return
     */
    private int close()
    {
        int status = 0;
        try {
            conn.close();
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return status;
    }

    /***
     * 获取表列表
     * @return
     */
    public String[] listTables() {

        ArrayList<String> tables = new ArrayList<String>();
        try {
            HTableDescriptor[] hTableDescriptors = admin.listTables();
            for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
                tables.add(hTableDescriptor.getNameAsString());
            }
            close();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return tables.toArray(new String[tables.size()]);
    }

    /**创建表
     * @param tbName
     * @return
     */
    private int createTable(String tbName, String[] cls) {
        int status = 0;
        
        return status;
    }

    /**
     * 删除表
     * @param tbName
     * @return
     */
    private int delTable(String tbName) {
        int status = 0;
        return status;
    }

    /***
     * 插入数据
     * @param tbName
     * @param colFamily
     * @param col
     * @param value
     * @return
     */
    private int insertData(String tbName, String rowKey, String colFamily, String col, String value) {
        int status = 0;

        return status;
    }

    /***
     * get数据
     * @param tbName
     * @param rowKey
     * @param colFamily
     * @param col
     */
    private void getData(String tbName, String rowKey, String colFamily, String col) {

    }

    /***
     * 删除数据
     * @param tbName
     * @param rowKey
     * @return
     */
    private int delData(String tbName, String rowKey) {
        int status = 0;

        return status;
    }

    /***
     * 更新数据
     * @param tbName
     * @param rowKey
     * @param colFamily
     * @param col
     * @param value
     * @return
     */
    private int updateData(String tbName, String rowKey, String colFamily, String col, String value)
    {
        int status = 0;
        status = insertData( tbName,  rowKey,  colFamily,  col,  value);
        return status;
    }

    /**
     * scandata
     * @param tbName
     * @param start
     * @param end
     */
    private void scanData(String tbName, String start, String end)
    {

    }
}
