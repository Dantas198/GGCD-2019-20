import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.stream.StreamSupport;

public class Ex3 {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("simplestream");
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(60));

        JavaPairRDD<String, String> mTitle = sc.sparkContext().textFile("/Users/dantas/Desktop/4ºAno/2ºsemestre/GGCD/downloads/imdb/title.basics.tsv.gz")
                .map(l -> l.split("\\t"))
                .filter(l -> !l[0].equals("tconst"))
                .mapToPair(l -> new Tuple2<>(l[0], l[2]));


        sc.socketTextStream("localhost", 12345)
                .window(Durations.seconds(600),Durations.seconds(60))
                .map(l -> l.split("\t"))
                .mapToPair(l -> new Tuple2<>(l[0], l[1]))
                .groupByKey()
                .mapToPair(t -> {
                    Double d = StreamSupport.stream(t._2.spliterator(),true)
                            .mapToDouble(Double::parseDouble)
                            .average()
                            .orElse(Double.NaN);
                    return new Tuple2<>(t._1, d);
                })
                .foreachRDD(rdd -> {
                    rdd.join(mTitle)
                        .mapToPair(p -> new Tuple2<>(p._2._1, p._2._2))
                        .sortByKey(false).take(3);
                });
        sc.start();
        sc.awaitTermination();
    }
}
