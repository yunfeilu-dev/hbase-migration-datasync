import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;


public class Main {
    public static void main(String args[]) {
        Configuration config = HBaseConfiguration.create();
        config.addResource("/etc/hbase/conf/hbase-site.xml");
        try {


            Connection connection = ConnectionFactory.createConnection(config);

            String filePath = "/home/hadoop/yunfeilu/output.file";
            byte[] bytes = Files.readAllBytes(Paths.get(filePath));
            Table table = connection.getTable(TableName.valueOf("user"));
            ArrayList<Put> arrayList = new ArrayList<Put>();

            Integer i = 4977471;
            while (true) {
                Put put = new Put(Bytes.toBytes(i.toString()));
                put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(i.toString()));
                put.add(Bytes.toBytes("info"), Bytes.toBytes("file"), bytes);
                arrayList.add(put);
                if (i % 50 == 0) {
                    table.put(arrayList);
                    arrayList = new ArrayList<>();
                }
                i += 1;
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

