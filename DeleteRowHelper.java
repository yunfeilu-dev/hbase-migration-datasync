import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;


public class DeleteRowHelper {
    public static void main(String args[]) {
        Configuration config = HBaseConfiguration.create();
        config.addResource("/etc/hbase/conf/hbase-site.xml");
        try {
            Connection connection = ConnectionFactory.createConnection(config);

            Table table = connection.getTable(TableName.valueOf("user"));
            ArrayList<Delete> arrayList = new ArrayList<>();

            Integer i = 0;
            while (true) {

                if (i % 2 == 1) {
                    Delete delete = new Delete(Bytes.toBytes(i.toString()));
                    arrayList.add(delete);
                }

                if (arrayList.size() == 50){
                    table.delete(arrayList);
                    arrayList = new ArrayList<>();
                    Thread.sleep(1000);
                }
                i += 1;
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

