import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class ActorsMRMain {

    public static void createTable(Connection conn, String tableName, String... families) throws IOException {
        Admin admin = conn.getAdmin();
        HTableDescriptor t = new HTableDescriptor(TableName.valueOf(tableName));
        for(String s : families)
            t.addFamily(new HColumnDescriptor(s));
        admin.createTable(t);
        admin.close();
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //TODO ideias
        // usar profiler para otimizações
        // usar ficheiros intermédios com o formato em bytes
        // sort-global
        // input splits e block size
        // comparação entre "cachar" os joins
        // concorrência de "partições" de reduce
        // map side join


        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "zoo");
        Connection conn = ConnectionFactory.createConnection(conf);

        createTable(conn, "actors", "info");
        // Actor info

        LoadActorInfo.executeJob(conf,
                "load_actors",
                "hdfs://namenode:9000/input/name.basics.tsv",
                "actors");

        ActorsTop3.executeMovieInfoJob(conf,
                "movies",
                "hdfs://namenode:9000/input/title.ratings.tsv",
         "hdfs://namenode:9000/outputJoinMovieInfo");

        ActorsTop3.executeActorWithMovieInfoJob(conf,
                "hdfs://namenode:9000/outputJoinMovieInfo",
                "hdfs://namenode:9000/input/title.principals.tsv",
                "hdfs://namenode:9000/outputJoinMovieInfoActor");

        ActorsTop3.executeTop3Job(conf, "top3",
                "hdfs://namenode:9000/outputJoinMovieInfoActor",
                "actors");

        Collaborators.executeActorByActorJob(conf,
                "hdfs://namenode:9000/outputJoinMovieInfoActor",
                "hdfs://namenode:9000/outputCollabsAux");
        Collaborators.executeCollabsJob(conf,
                "collaborators_job",
                "hdfs://namenode:9000/outputCollabsAux",
                "actors");
    }
}
