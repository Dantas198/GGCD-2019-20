import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Random;
import java.io.IOException;

public class Step2 {
    public static void castVote(Table ht, int movieId, long vote) throws IOException {
        Increment incr = new Increment(Bytes.toBytes("FILME" + movieId + "&" + vote));

        incr.addColumn(Bytes.toBytes("qual"), Bytes.toBytes("q"), 1L);
        ht.increment(incr);
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        Table ht = conn.getTable(TableName.valueOf("filmes"));

        Random rand = new Random();
        long duration = 0;
        int numberOfVotes = 10000;

        for(int i=0; i<numberOfVotes; i++){
            int randVote = rand.nextInt(10);// Gera um inteiro de 0 a 9
            randVote++;// para impedir que o randVote seja zero

            long startTimeIter = System.nanoTime();
            //neste caso só existem votações para o filme 2
            castVote(ht, 2 , randVote);
            long durationIter = System.nanoTime() - startTimeIter;

            //System.out.println("The duration of vote number " + i + " is: " + durationIter/1000000000 + " in seconds");
            duration += durationIter;
        }


        System.out.println("The time needed to cast " + numberOfVotes + " votes are: " + duration/1000000000 + " in seconds");

        ht.close();
        conn.close();
    }
}
