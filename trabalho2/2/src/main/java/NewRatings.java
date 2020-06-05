/**
 * Este codigo resolve a questão 2.C:
 * Ratings:Atualize o ficheirotitle.ratings.tsvtendo em conta o seu conteúdo ante-rior e os novos votos recebidos até ao momento.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class NewRatings {
    public static void merge(String srcPath, String dstPath) throws IOException, URISyntaxException{
        Configuration c = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://namenode:9000"), c);
        FileUtil.copyMerge(fileSystem, new Path(srcPath), fileSystem, new Path(dstPath), false, c, null);
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("simplestream");
        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaPairRDD<String, Tuple2<Double, Integer>> old_rattings = sc.textFile("hdfs://namenode:9000/input/title.ratings.tsv")
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("tconst"))
                .mapToPair(l -> new Tuple2<>(l[0], new Tuple2<>(Double.parseDouble(l[1]), Integer.parseInt(l[2]))));

        sc.textFile("ex1Output/*")
                .map(l -> l.split("\t"))
                .mapToPair(l -> new Tuple2<>(l[0], new Tuple2<>(Double.parseDouble(l[1]), 1)))
                .reduceByKey((t1, t2) -> new Tuple2<>(t1._1 + t2._1, t1._2 + t2._2))
                .rightOuterJoin(old_rattings)//<ID, ((new_ratting, new_count),  (old_ratting, old_count))>
                .map(p -> {
                    Tuple2<Double, Integer> old_pairs = p._2._2;
                    Optional<Tuple2<Double, Integer>> new_optional_pairs = p._2._1;
                    if(p._2._1.isPresent()){
                        int old_numVotes = old_pairs._2;
                        double old_ratting = old_pairs._1 * old_numVotes;

                        Tuple2<Double, Integer> new_pairs = new_optional_pairs.get();
                        int new_numVotes = new_pairs._2;
                        double new_ratting = new_pairs._1;

                        int current_numVotes = old_numVotes + new_numVotes;
                        double current_ratting = (old_ratting + new_ratting) / current_numVotes;

                        return p._1 + "\t" + current_ratting + "\t" + current_numVotes;
                    }
                    else{
                        return p._1 + "\t" + old_pairs._1 + "\t" + old_pairs._2;
                    }
                })
                .saveAsTextFile("hdfs://namenode:9000/ex2Output/2c_tmp");

        try {
            merge("hdfs://namenode:9000/ex2Output/title.ratings.new.tsv", "hdfs://namenode:9000/ex2Output/2c_tmp");
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}
