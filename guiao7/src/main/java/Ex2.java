import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.stream.StreamSupport;

public class Ex2 {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("simplestream");

        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(60));
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
                return new Tuple2<>(d, t._1);
            })
            .foreachRDD(rdd -> {
                System.out.println("Top 3 :" + rdd.sortByKey(false).take(3).toString());
            });

        sc.start();
        sc.awaitTermination();
    }
}
