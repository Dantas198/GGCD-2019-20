import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Comparator;
import java.util.List;


public class Ex1 {

    public static class FComparator implements Comparator<Tuple2<String,Float>>, Serializable {
        public int compare(Tuple2<String,Float> t1,
                           Tuple2<String, Float> t2) {
            return Float.compare(t2._2, t1._2);
        }
    }

    public static String getDateTime(Time time){
        Instant instant = Instant.ofEpochMilli(time.milliseconds());
        String t = instant.atZone(ZoneId.systemDefault()).toLocalDateTime().withSecond(0).toString();
        System.out.println(t);
        return t.replace(":", "_");
    }

    public static long getTime(Time time){
        return (time.milliseconds() / (1000 * 60)) % 60;
    }

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("simplestream");

        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate("hdfs://namenode:9000/tmp/stream", () -> {
            JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(10));

            JavaPairRDD<String, String> mTitle = sc.sparkContext().textFile("hdfs://namenode:9000/input/title.basics.tsv")
                .map(l -> l.split("\\t"))
                .filter(l -> !l[0].equals("tconst"))
                .mapToPair(l -> new Tuple2<>(l[0], l[2]))
                .cache();

            JavaDStream<String[]> fst =sc.socketTextStream("172.18.0.1", 12345)
                .map(l -> l.split("\t"))
                .cache();

            JavaPairDStream<String, Tuple2<Tuple2<Float, Integer>, String>> snd = fst.mapToPair(l -> new Tuple2<>(l[0], new Tuple2<>(Float.parseFloat(l[1]), 1)))
                .transformToPair(rdd -> rdd.join(mTitle))
                .cache();


            fst.transformToPair((rdd, time) -> rdd.mapToPair(l -> new Tuple2<>(new Tuple2<>(l[0], l[1]), getTime(time))))
                .window(Durations.seconds(60), Durations.seconds(60))
                .foreachRDD((rdd, time) -> rdd.saveAsTextFile("hdfs://namenode:9000/ex1Output/" + getDateTime(time) + "/"));


            snd.mapToPair(p -> new Tuple2<>(new MovieKey(p._1, p._2._2), new Tuple2<>(p._2._1._1, p._2._1._2)))
                .reduceByKeyAndWindow(
                    (p1, p2) -> new Tuple2<>(p1._1 + p2._1, p1._2 + p2._2),
                    (p1, p2) -> new Tuple2<>(p1._1 - p2._1, p1._2 - p2._2),
                    Durations.seconds(60),
                    Durations.seconds(10))
                .filter(p -> p._2._2 > 0)
                .mapToPair(p -> new Tuple2<>(p._1.movieName, p._2._1 / p._2._2))
                .foreachRDD(rdd -> System.out.println("Top 3: " + rdd.takeOrdered(3, new FComparator()).toString()));


            //TODO perguntar ao stor se há maneira de uma janela maior usar o calculo de uma mais pequena

            snd.mapToPair(l -> new Tuple2<>(new MovieKey(l._1, l._2._2), l._2._1._2))
                .reduceByKeyAndWindow(
                    Integer::sum,
                    Durations.seconds(60),
                    Durations.seconds(60))
                .mapWithState(StateSpec.function((MovieKey k, Optional<Integer> v, State<Integer> s)->{
                    int old_count = s.exists() ? s.get() : 0;
                    s.update(v.get());
                    return v.get() > old_count ? Optional.of(k.movieName) : Optional.empty();
                }))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .print();

            sc.checkpoint("hdfs://namenode:9000/tmp/stream");
            return sc;
        });

        jsc.start();
        jsc.awaitTermination();
    }
}
