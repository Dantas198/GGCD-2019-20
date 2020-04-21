import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import timer.Timer;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //TODO ideias
        // usar profiler para otimizações
        // usar ficheiros intermédios com o formato em bytes
        // sort-global
        // input splits e block size
        // comparação entre "cachar" os joins
        // concorrência de "partições" de reduce
        // map side join

    /*
    Criar tabelas só uma vez:
        docker run -it --network docker-hbase_default --env-file hbase-distributed-local.env bde2020/hbase-base hbase shell
            disable 'actors'
            drop 'actors'
        docker run --network docker-hbase_default --env-file hadoop.env  bde2020/hadoop-base hdfs dfs -rm -r /outputJoinMovieInfo
        docker run --network docker-hbase_default --env-file hadoop.env  bde2020/hadoop-base hdfs dfs -rm -r /outputJoinMovieInfoActor
     */


        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "zoo");
        Connection conn = ConnectionFactory.createConnection(conf);
        Timer t = new Timer(TimeUnit.SECONDS);
/*
        // Create tables
        LoadInfo.createTable(conn, "movies", "info");
        LoadInfo.createTable(conn, "actors", "info");

        // Load infos
        t.addCheckpoint("start");
        LoadInfo.executeLoadActorJob(conf,
                "load_actors",
                "hdfs://namenode:9000/input/name.basics.tsv",
                "actors");
        t.addCheckpoint("load actor info");
        LoadInfo.executeLoadMovieJob(conf,
                "load_movies",
                "hdfs://namenode:9000/input/title.basics.tsv",
                "movies");
        t.addCheckpoint("load movie info");
*/
        ActorsTop3.executeMovieInfoJob(conf,
                "movies",
                "hdfs://namenode:9000/input/title.ratings.tsv",
         "hdfs://namenode:9000/outputJoinMovieInfo");
        t.addCheckpoint("join ratings with title from movie");
        ActorsTop3.executeActorWithMovieInfoJob(conf,
                "hdfs://namenode:9000/outputJoinMovieInfo",
                "hdfs://namenode:9000/input/title.principals.tsv",
                "hdfs://namenode:9000/outputJoinMovieInfoActor");
        t.addCheckpoint("join actor name with previous info");
        /*
        //utilizando um sort secundário
        ActorsTop3.executeTop3WithSecundarySortJob(conf, "top3",
                "hdfs://namenode:9000/outputJoinMovieInfoActor",
                "actors");
        */

         /*
        //sem sort secundário e incluindo a contagem
        ActorsTop3.executeTop3WithCountJob(conf, "top3",
                "hdfs://namenode:9000/outputJoinMovieInfoActor",
                "actors");
        */


        // contagem feita à parte
        ActorsTop3.executeTop3Job(conf, "top3",
                "hdfs://namenode:9000/outputJoinMovieInfoActor",
                "actors");
        t.addCheckpoint("top 3");
        MovieCount.executeMovieCountJob(conf,
                 "movie_count",
                 "hdfs://namenode:9000/input/title.principals.tsv",
                 "actors");
        t.addCheckpoint("separated movie count");

/*
        // Cálculo dos colaboradores
        Collaborators.executeActorByActorJob(conf,
                "hdfs://namenode:9000/outputJoinMovieInfoActor",
                "hdfs://namenode:9000/outputCollabsAux");
        t.addCheckpoint("actor by actor job");
        Collaborators.executeCollabsJob(conf,
                "collaborators_job",
                "hdfs://namenode:9000/outputCollabsAux",
                "actors");
        t.addCheckpoint("collaborators");
 */
        t.print();
    }
}
