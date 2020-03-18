import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Step2 {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        Table ht = conn.getTable(TableName.valueOf("filmes"));

        //filme0 e um voto na classificação 9 como exemplo
        Increment incr = new Increment(Bytes.toBytes("FILME0&9"));

        incr.addColumn(Bytes.toBytes("qual"), Bytes.toBytes("q"), 1L);
        ht.increment(incr);

        ht.close();
        conn.close();
    }
}
