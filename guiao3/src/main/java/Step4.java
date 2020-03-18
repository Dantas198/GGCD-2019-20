import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Step4 {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        Table ht = conn.getTable(TableName.valueOf("actor"));

        Scan s = new Scan();
        int i = 0;
        for(Result r: ht.getScanner(s)) {
            // actor
            String key = Bytes.toString(r.getRow());
            String[] keyParts = key.split("&");

            System.out.println("actor: " + keyParts[0]);

            byte [] value1 = r.getValue(Bytes.toBytes("dates"), Bytes.toBytes("b"));
            String str = Bytes.toString(value1);
            System.out.println("Birth date with b == " + keyParts[1] + " : " + str);

            value1 = r.getValue(Bytes.toBytes("dates"), Bytes.toBytes("d"));
            str = Bytes.toString(value1);
            System.out.println("Birth date with d == " + keyParts[2] + " : " + str);

            value1 = r.getValue(Bytes.toBytes("nmovies"), Bytes.toBytes("nm"));
            str = Bytes.toString(value1);
            System.out.println("Birth date with nm == " + keyParts[3] + " : " + str);

            value1 = r.getValue(Bytes.toBytes("top3"), Bytes.toBytes("t"));
            str = Bytes.toString(value1);
            System.out.println("Birth date with t == " + keyParts[4] + " : " + str);

            value1 = r.getValue(Bytes.toBytes("collab"), Bytes.toBytes("c"));
            str = Bytes.toString(value1);
            System.out.println("Birth date with c == " + keyParts[5] + " : " + str);

        }
        ht.close();
        conn.close();
    }
}
