package com.lixiang.hbase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


/**
 * @Description //TODO
 * @Author 李项
 * @Date 2020/4/5
 * @Version 1.0
 */
public class HbaseUtil {
    private static Connection conn=null;
    private  static ThreadLocal<Connection> context=new ThreadLocal<Connection> ();

    private HbaseUtil(){

    }
    public static void getConnection() throws IOException {
        if(context.get()==null) {
            Configuration configuration = HBaseConfiguration.create();
            conn = ConnectionFactory.createConnection(configuration);
            context.set(conn);
        }
    }

    public static void  close() throws IOException {
        Connection connection = context.get();

        if(connection != null) {
            connection. close();
            context.remove();
        }
    }

    public static void insertData(String tableName,String rowKey,String family,String column,String value) throws IOException {
        Connection connection = context.get();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family),Bytes.toBytes(column),Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

}
