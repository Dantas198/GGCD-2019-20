import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import timer.Timer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Ex2_V2 {

    static String ex1_output_dir = "hdfs://namenode:9000/ex1Output/";
    static String ex2_output_dir = "hdfs://namenode:9000/ex2Output/";
    JavaSparkContext sc;

    public Ex2_V2(JavaSparkContext sc){
        this.sc = sc;
    }

    public static class SizeComparator implements Comparator<Tuple2<String,Integer>>, Serializable {
        public int compare(Tuple2<String, Integer> t1,
                           Tuple2<String, Integer> t2) {
            return Integer.compare(t2._2, t1._2);
        }
    }

    public static List<Tuple2<String, Integer>> top10_v1(JavaPairRDD<String, String> infosByMovieId){
        return infosByMovieId.aggregateByKey(new HashSet<String>(),
                    (hs, movie) -> {hs.add(movie); return hs;},
                    (hs1, hs2) -> {hs1.addAll(hs2); return hs1;})
                .map(t -> new Tuple2<>(t._1, t._2.size()))
                .takeOrdered(10, new SizeComparator());
    }

    public static JavaPairRDD<String, Iterable<Tuple2<String, String>>> collaborators_v1(JavaPairRDD<String, Tuple2<ActorKey, String>> infosByMovieId){
        return infosByMovieId.groupByKey()
                .flatMapToPair(l -> {
                    List<Tuple2<ActorKey, String>> list = StreamSupport.stream(l._2.spliterator(), true).collect(Collectors.toList());
                    List<Tuple2<ActorKey, Tuple2<String, String>>> result = new ArrayList<>();
                    for(int i=0; i<list.size(); i++)
                        for(int j = 0; j<list.size(); j++)
                            if(i!=j)
                                result.add(new Tuple2<>(list.get(i)._1, new Tuple2<>(list.get(j)._1.actorName, list.get(j)._2)));
                    return result.iterator();
                })
                .groupByKey()
                .mapToPair(p -> new Tuple2<>(p._1.actorName, p._2));
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Exerciocio 2_v2");
        JavaSparkContext sc = new JavaSparkContext(conf);

         JavaPairRDD<String, String> actorNames = sc.textFile("hdfs://namenode:9000/input/name.basics.tsv", 8)
                .map(l -> l.split("\t+"))
                .filter( l -> !l[0].equals("nconst"))
                .mapToPair(l -> new Tuple2<>(l[0], l[1]))
                .cache();

        JavaPairRDD<String, String> movieIdByActorId = sc.textFile("hdfs://namenode:9000/input/title.principals.tsv", 8)
                .map(l -> l.split("\t+"))
                .filter(l -> !l[0].equals("tconst") && (l[3].equals("actor") || l[3].contains("actress") || l[3].contains("self")))
                .mapToPair(l -> new Tuple2<>(l[2], l[0]))
                .cache();

        List<Tuple2<String, Integer>> top10 = top10_v1(movieIdByActorId);

        List<Tuple2<String, Integer>> top10WithNames = sc.parallelize(top10, 8)
            .mapToPair(l->l)
            .join(actorNames)
            .map(l -> new Tuple2<>(l._2._2, l._2._1))
            .collect();

        Timer t = new Timer(TimeUnit.SECONDS);
        t.start();
        System.out.println("TOPPPPPPP 10: " + top10WithNames.toString());
        t.addCheckpoint("top 10 time");
        t.print();

    }

}
