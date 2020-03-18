import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Random;

public class Step3 {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        // criar tabela
        Admin admin = conn.getAdmin();
        HTableDescriptor t = new HTableDescriptor(TableName.valueOf("actor"));
        t.addFamily(new HColumnDescriptor("info"));
        //t.addFamily(new HColumnDescriptor("collab"));
        admin.createTable(t);
        admin.close();

        Table ht = conn.getTable(TableName.valueOf("actor"));

        Random rand = new Random(100);
        for(int i=0; i<10; i++){
            Put put = new Put(Bytes.toBytes(i));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"),
                    Bytes.toBytes("abcd"+i));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("birth"),
                    Bytes.toBytes("12" + i +  "-" + "10" + "1998" + i));

            String death;
            if(i == 9)
                death = "none";
            else{
                death = "13" + "-" + "11" + "-" + "2005" + i;
            }

            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("death"),
                    Bytes.toBytes(death));

            StringBuilder sb = new StringBuilder();
            for(int j=1; j<4; j++){
                String frank = "movie" + rand.nextInt(1000) + 1;
                sb.append(frank);
                if(j!=3)
                    sb.append("%");
            }

            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("mrank"),
                    Bytes.toBytes(sb.toString()));

            int k = rand.nextInt(13);
            StringBuilder sb2 = new StringBuilder();
            for(int j=1 ; j<=4; j++){
                String collab = "col" + rand.nextInt(1000) + 1;
                sb2.append(collab);
                if(j!=k)
                    sb2.append("%");
            }

            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("collab"),
                    Bytes.toBytes(sb2.toString()));

            ht.put(put);
        }

        ht.close();
        conn.close();
    }
}
