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
public class TestHbaseAPI_1 {
    public static void main(String[] args) throws IOException {
        //通过java访问HBASE数据库
        //1)获取hbase连接对象
        Configuration configuration = HBaseConfiguration.create();
       // configuration.set("hbase.rootdir", "hdfs://192.168.178.30:9000/home/app/data/hbase_db");
        /*configuration.set("hbase.master", "lixiang.ac.cn:16000");
        configuration.set("hbase.zookeeper.quorum","lixiang.ac.cn:2182,lixiang.ac.cn:2183");
        configuration.set("hbase.zookeeper.property.clientPort","2183"); //端口号*/

    //configuration.set("hbase.master", "lixiang:16000");
//        configuration.set("zookeeper.znode.parent","/hbase-unsecure");
        configuration.set("hbase.zookeeper.quorum","lixiang");  //hbase 服务地址
        configuration.set("hbase.zookeeper.property.clientPort","2181"); //端口号
        configuration.setInt("hbase.rpc.timeout",20000);
        configuration.setInt("hbase.client.operation.timeout",30000);
        configuration.setInt("hbase.client.scanner.timeout.period",200000);
        Connection connection = ConnectionFactory.createConnection(configuration);
        System.out.println(connection);
        //2)获取操作对象
        Admin admin = connection.getAdmin();
        //获取命名空间
        try {
            NamespaceDescriptor namespace = admin.getNamespaceDescriptor("atguigu");
        }catch (NamespaceNotFoundException e){
            //创建表空间
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create("atguigu").build();
            admin.createNamespace(namespaceDescriptor);
        }
        //判断hbase中是否存在某张表
        TableName tableName = TableName.valueOf("atguigu:studnet");
        boolean flag = admin.tableExists(tableName);

        if(flag){
            //查询数据
            //获取指定的表对象
            Table table = connection.getTable(tableName);
            String rowKey="1001";
            //字符编码
            Result result = table.get(new Get(Bytes.toBytes(rowKey)));
            if( result.isEmpty()){ //是否为空
                //新增数据
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes("张三"));
                table.put(put);
                System.out.println("增加数据。。。。");
            }else{
                //展示数据
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
                    System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
                    System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
                    System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)) );
                }
            }
        }else {
            //创建表
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            //增加列族
            HColumnDescriptor cd=new HColumnDescriptor("info");
            tableDescriptor.addFamily(cd);
            admin.createTable(tableDescriptor);
            System.out.println("表格创建成功。。。。。。。");
        }

        //3)操作数据库
        //4)获取操作结果
        //5)关闭数据库连接
    }
}
