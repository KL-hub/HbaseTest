package com.lixiang.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @Description //TODO
 * @Author 李项
 * @Date 2020/4/2
 * @Version 1.0
 */
public class TestHbaseAPI_2 {
    public static void main(String[] args) throws IOException {//通过java访问HBASE数据库
        //1)获取hbase连接对象
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "lixiang");  //hbase 服务地址
        configuration.set("hbase.zookeeper.property.clientPort", "2181"); //端口号
        configuration.setInt("hbase.rpc.timeout", 20000);
        configuration.setInt("hbase.client.operation.timeout", 30000);
        configuration.setInt("hbase.client.scanner.timeout.period", 200000);
        Connection connection = ConnectionFactory.createConnection(configuration);

        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf("atguigu:student");
        if(admin.tableExists(tableName)){
            //删除表
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        
        //删除数据
        Table table = connection.getTable(tableName);
        String rowKey="1001";
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);

        //查询多条数据
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            //展示数据
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)) );
            }
        }



    }
}
