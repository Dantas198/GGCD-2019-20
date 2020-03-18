import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class VisualizeStep1 {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        Table ht = conn.getTable(TableName.valueOf("filmes"));

        Scan s = new Scan();
        int i = 0;
        float tscore = 0;
        float tvotes = 0;
        for(Result r: ht.getScanner(s)) {
            // Filme
            String key = Bytes.toString(r.getRow());
            String[] keyParts = key.split("&");
            if(i == 0)
                System.out.println("MOVIE: " + keyParts[0]);

            byte [] value1 = r.getValue(Bytes.toBytes("qual"), Bytes.toBytes("q"));
            Long votes = Bytes.toLong(value1);
            System.out.println("Number of votes with q == " + keyParts[1] + " : " + votes);
            i++;
            tscore += Integer.parseInt(keyParts[1]) * votes;
            tvotes += votes;
            if(i == 10) {
                System.out.println("Score: " + tscore/tvotes);
                i = 0;
                tscore = tvotes = 0;

            }
        }
        ht.close();
        conn.close();
    }
}
