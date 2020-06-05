import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.StreamSupport;


public class Ex1 {

    public static class FComparator implements Comparator<Tuple2<String,Double>>, Serializable {
        public int compare(Tuple2<String, Double> t1,
                           Tuple2<String, Double> t2) {
            return Double.compare(t2._2, t1._2);
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


    public static void log_v1(JavaPairDStream<String, Tuple2<Tuple2<Double, Integer>, String>> fst){
        fst.transform((rdd, time) -> rdd.map(l -> l._1 + "\t" + l._2._1._1 + "\t" + getTime(time)))
                .window(Durations.seconds(60 * 5), Durations.seconds(60 * 5))
                .foreachRDD((rdd, time) -> rdd.saveAsTextFile("hdfs://namenode:9000/ex1Output/" + getDateTime(time) + "/"));
    }

    public static void top3_v1(JavaPairDStream<String, Tuple2<Tuple2<Double, Integer>, String>> fst) {
        fst.mapToPair(p -> new Tuple2<>(new MovieKey(p._1, p._2._2), new Tuple2<>(p._2._1._1, p._2._1._2)))
            .reduceByKeyAndWindow(
                (p1, p2) -> new Tuple2<>(p1._1 + p2._1, p1._2 + p2._2),
                (p1, p2) -> new Tuple2<>(p1._1 - p2._1, p1._2 - p2._2),
                Durations.seconds(60 * 5),
                Durations.seconds(60))
            .filter(p -> p._2._2 > 0)
            .mapToPair(p -> new Tuple2<>(p._1.movieName, p._2._1 / p._2._2))
            .foreachRDD(rdd -> System.out.println("TTTTTTTTTTTTTTTTTTop 3: " + rdd.takeOrdered(3, new FComparator()).toString()));
    }

    // no inverse function
    public static void top3_v2(JavaPairDStream<String, Tuple2<Tuple2<Double, Integer>, String>> fst) {
        fst.mapToPair(p -> new Tuple2<>(new MovieKey(p._1, p._2._2), new Tuple2<>(p._2._1._1, p._2._1._2)))
                .reduceByKeyAndWindow(
                        (p1, p2) -> new Tuple2<>(p1._1 + p2._1, p1._2 + p2._2),
                        Durations.seconds(60 * 10),
                        Durations.seconds(60))
                .mapToPair(p -> new Tuple2<>(p._1.movieName, p._2._1 / p._2._2))
                .foreachRDD(rdd -> System.out.println("TTTTTTTTTTTTTTTTTTop 3: " + rdd.takeOrdered(3, new FComparator()).toString()));
    }

    // reduce separated of window
    public static void top3_v3(JavaPairDStream<String, Tuple2<Tuple2<Double, Integer>, String>> fst) {
        fst.mapToPair(p -> new Tuple2<>(new MovieKey(p._1, p._2._2), new Tuple2<>(p._2._1._1, p._2._1._2)))
                .window(Durations.seconds(60 * 10), Durations.seconds(60))
                .reduceByKey((t1, t2) -> new Tuple2<>(t1._1 + t2._1, t1._2 + t2._2))
                .mapToPair(p -> new Tuple2<>(p._1.movieName, p._2._1 / p._2._2))
                .foreachRDD(rdd -> System.out.println("TTTTTTTTTTTTTTTTTTop 3: " + rdd.takeOrdered(3, new FComparator()).toString()));
    }


    //using groupby, java stream and takeOrdered 3
    public static void top3_v4(JavaPairDStream<String, Tuple2<Tuple2<Double, Integer>, String>> fst) {
        fst.mapToPair(p -> new Tuple2<>(new MovieKey(p._1, p._2._2), new Tuple2<>(p._2._1._1, p._2._1._2)))
                .window(Durations.seconds(60 * 10), Durations.seconds(60))
                .groupByKey()
                .mapToPair(t -> {
                    Double d = StreamSupport.stream(t._2.spliterator(),true)
                            .mapToDouble(x -> x._1)
                            .average()
                            .orElse(Double.NaN);
                    return new Tuple2<>(t._1.movieName, d);
                })
                .foreachRDD(rdd -> System.out.println("TTTTTTTTTTTTTTTTTTop 3: " + rdd.takeOrdered(3, new FComparator()).toString()));
    }

    //using v3 plus sort at the end
    public static void top3_v5(JavaPairDStream<String, Tuple2<Tuple2<Double, Integer>, String>> fst) {
        fst.mapToPair(p -> new Tuple2<>(new MovieKey(p._1, p._2._2), new Tuple2<>(p._2._1._1, p._2._1._2)))
                .window(Durations.seconds(60 * 10), Durations.seconds(60))
                .groupByKey()
                .mapToPair(t -> {
                    Double d = StreamSupport.stream(t._2.spliterator(),true)
                            .mapToDouble(x -> x._1)
                            .average()
                            .orElse(Double.NaN);
                    return new Tuple2<>(t._1.movieName, d);
                })
                .foreachRDD(rdd -> System.out.println("TTTTTTTTTTTTTTTTTTop 3: " + rdd.sortByKey(false).take(3).toString()));
    }

    public static void trending_v1(JavaPairDStream<String, Tuple2<Tuple2<Double, Integer>, String>> fst) {
        fst.mapToPair(l -> new Tuple2<>(new MovieKey(l._1, l._2._2), l._2._1._2))
            .reduceByKeyAndWindow(
                Integer::sum,
                Durations.seconds(60 * 6),
                Durations.seconds(60 * 6))
            .mapWithState(StateSpec.function((MovieKey k, Optional<Integer> v, State<Integer> s) -> {
                if (!v.isPresent())
                    return Optional.empty();
                int old_count = s.exists() ? s.get() : 0;
                s.update(v.get());
                return v.get() > old_count ? Optional.of(k.movieName) : Optional.empty();
            }))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .print();
    }

    //using updateStateByKey
    public static void trending_v2(JavaPairDStream<String, Tuple2<Tuple2<Double, Integer>, String>> fst) {
        fst.mapToPair(l -> new Tuple2<>(new MovieKey(l._1, l._2._2), l._2._1._2))
            .reduceByKeyAndWindow(
                Integer::sum,
                Durations.seconds(70),
                Durations.seconds(70))
            .updateStateByKey((List<Integer> vl, Optional<Tuple2<Boolean, Integer>> s) -> {
                if (vl.isEmpty())
                    return Optional.empty();
                int old_count = s.isPresent() ? s.get()._2 : 0;
                //apenas um valor por causa do reduce
                int new_count = vl.get(0);
                return new_count > old_count ? Optional.of(new Tuple2<>(true, new_count)) : Optional.of(new Tuple2<>(false, new_count));
            })
            .filter(t -> t._2._1)
            .print();
    }


    //using updateStateByKey and local reduce
    public static void trending_v3(JavaPairDStream<String, Tuple2<Tuple2<Double, Integer>, String>> fst) {
        fst.mapToPair(l -> new Tuple2<>(new MovieKey(l._1, l._2._2), l._2._1._2))
                .window(Durations.seconds(70), Durations.seconds(70))
                .updateStateByKey((List<Integer> vl, Optional<Tuple2<Boolean, Integer>> s) -> {
                    if (vl.isEmpty())
                        return Optional.empty();
                    int old_count = s.isPresent() ? s.get()._2 : 0;
                    //apenas um valor por causa do reduce
                    int new_count = vl.stream().reduce(0, Integer::sum);
                    return new_count > old_count ? Optional.of(new Tuple2<>(true, new_count)) : Optional.of(new Tuple2<>(false, new_count));
                })
                .filter(t -> t._2._1)
                .print();
    }

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("simplestream");

        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate("hdfs://namenode:9000/tmp/stream", () -> {
            JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(60));

            JavaPairRDD<String, String> mTitle = sc.sparkContext().textFile("hdfs://namenode:9000/input/title.basics.tsv")
                .map(l -> l.split("\t+"))
                .filter(l -> !l[0].equals("tconst"))
                .mapToPair(l -> new Tuple2<>(l[0], l[2]))
                .cache();


            //TODO using join at the end?

            JavaPairDStream<String, Tuple2<Tuple2<Double, Integer>, String>> fst = sc.socketTextStream("172.18.0.1", 12345)
                .map(l -> l.split("\t+"))
                .mapToPair(l -> new Tuple2<>(l[0], new Tuple2<>(Double.parseDouble(l[1]), 1)))
                .transformToPair(rdd -> rdd.join(mTitle))
                .cache();

            log_v1(fst);
            top3_v1(fst);
            trending_v1(fst);

            sc.checkpoint("hdfs://namenode:9000/tmp/stream");
            return sc;
        });

        jsc.start();
        jsc.awaitTermination();
    }
}
