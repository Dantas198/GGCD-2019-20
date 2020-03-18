import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class Step4 {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        Table ht = conn.getTable(TableName.valueOf("actor"));

        //Exemplo de visualização da página no ator com id 9
        Get g = new Get(toBytes(2));
        Result r = ht.get(g);
        System.out.println(r);

        String name = Bytes.toString(r.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")));
        System.out.println("Name: " + name);

        String bdate = Bytes.toString(r.getValue(Bytes.toBytes("info"), Bytes.toBytes("birth")));
        System.out.println("Birth date: " + bdate);

        String ddate = Bytes.toString(r.getValue(Bytes.toBytes("info"), Bytes.toBytes("death")));
        if(ddate.equals("none"))
            System.out.println("Death date: Still alive!");
        else
            System.out.println("Death date: " + ddate);

        String movies = Bytes.toString(r.getValue(Bytes.toBytes("info"), Bytes.toBytes("mrank")));
        String[] moviesByRank = movies.split("%");

        System.out.println("Movie Rank");
        int i = 1;
        for(String s : moviesByRank){
            System.out.println("Rank " + i + " : " + s);
            i++;
        }

        String c = Bytes.toString(r.getValue(Bytes.toBytes("info"), Bytes.toBytes("collab")));
        String[] collabs = c.split("%");

        System.out.println("Collaborators");
        for(String s : collabs){
            System.out.println("Rank " + i + " : " + s);
            i++;
        }


        ht.close();
        conn.close();
    }
}
