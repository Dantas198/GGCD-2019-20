import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Step3 {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        // criar tabela
        Admin admin = conn.getAdmin();
        HTableDescriptor t = new HTableDescriptor(TableName.valueOf("actor"));
        t.addFamily(new HColumnDescriptor("name"));
        t.addFamily(new HColumnDescriptor("birth"));
        t.addFamily(new HColumnDescriptor("death"));
        t.addFamily(new HColumnDescriptor("nmovies"));
        t.addFamily(new HColumnDescriptor("top3"));
        t.addFamily(new HColumnDescriptor("collab"));
        admin.createTable(t);
        admin.close();

        Table ht = conn.getTable(TableName.valueOf("actor"));
        for(int i=0; i<10; i++) {
            byte[] rowKey = Bytes.add(Bytes.toBytes("A" + i), Bytes.toBytes("&201" + i + "-" + i + "-1"), Bytes.toBytes("&201" + (i + 30) + "-" + i + "-1"),
                    Bytes.toBytes("&" + (14 + i)), Bytes.toBytes("&Movie" + i + ";Movie" + (i+1)), Bytes.toBytes("&"));
            Put put = new Put(rowKey);

            // Nao percebo a seguinte linha
            //put.addColumn(Bytes.toBytes("qual"), Bytes.toBytes("q"), Bytes.toBytes(0L));

            ht.put(put);
        }
        ht.close();
        conn.close();
    }
}
