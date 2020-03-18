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
        t.addFamily(new HColumnDescriptor("dates"));
        t.addFamily(new HColumnDescriptor("nmovies"));
        t.addFamily(new HColumnDescriptor("top3"));
        t.addFamily(new HColumnDescriptor("collab"));
        admin.createTable(t);
        admin.close();

        Table ht = conn.getTable(TableName.valueOf("actor"));
        for(int i=0; i<10; i++) {
            String collaborators;
            if(i==0){
                collaborators = "&A" + (i + 1);
            }
            else{
                if(i==9){
                    collaborators = "&A" + (i - 1);
                }
                else {
                    collaborators = "&A" + (i - 1) + ";A" + (i + 1);
                }
            }

            byte[] rowKey = Bytes.add(Bytes.toBytes("A" + i), Bytes.toBytes("&201" + i + "-" + i + "-1"), Bytes.toBytes("&201" + (i + 30) + "-" + i + "-1"),
                    Bytes.toBytes("&" + (14 + i)), Bytes.toBytes("&FILME" + i + ";FILME" + (i+1)), Bytes.toBytes(collaborators));

            Put put = new Put(rowKey);

            put.addColumn(Bytes.toBytes("dates"), Bytes.toBytes("b"), Bytes.toBytes(0L));
            put.addColumn(Bytes.toBytes("dates"), Bytes.toBytes("d"), Bytes.toBytes(0L));
            put.addColumn(Bytes.toBytes("nmovies"), Bytes.toBytes("nm"), Bytes.toBytes(0L));
            put.addColumn(Bytes.toBytes("top3"), Bytes.toBytes("t"), Bytes.toBytes(0L));
            put.addColumn(Bytes.toBytes("collab"), Bytes.toBytes("c"), Bytes.toBytes(0L));

            ht.put(put);
        }
        ht.close();
        conn.close();
    }
}
