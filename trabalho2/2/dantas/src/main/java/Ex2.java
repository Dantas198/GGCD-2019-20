import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.internal.io.HadoopWriteConfigUtil;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class Ex2 {


    static String ex1_output_dir = "hdfs://namenode:9000/ex1Output/";
    static String ex2_output_dir = "hdfs://namenode:9000/ex2Output/";
    static String ex2_c_output_dir = ex2_output_dir + "title.ratings.new.tsv";
    static String ex2_c_tmp_output_dir = ex2_output_dir + "2c_tmp";
    JavaSparkContext sc;

    public Ex2(JavaSparkContext sc){
        this.sc = sc;
    }


    public static void merge(String srcPath, String dstPath) throws IOException, URISyntaxException {
        Configuration c = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://namenode:9000"), c);
        FileUtil.copyMerge(fileSystem, new Path(srcPath), fileSystem, new Path(dstPath), false, c, null);
    }

    public <K, V> JavaPairRDD<K, V> genericMapOperation(String inputFile, Function<String[],Boolean> f1, PairFunction<String[],K,V> f2){
        return sc.textFile(inputFile)
                .map(l -> l.split("\t+"))
                .filter(f1)
                .mapToPair(f2);
    }

    public static class SizeComparator implements Comparator<Tuple2<ActorKey,Integer>>, Serializable {
        public int compare(Tuple2<ActorKey, Integer> t1,
                           Tuple2<ActorKey, Integer> t2) {
            return Integer.compare(t2._2, t1._2);
        }
    }

    //TODO extra de encontrar o filme com máxima cotação por ator
    //TODO joins no fim?

    public static List<Tuple2<ActorKey, Integer>> top10_v1(JavaPairRDD<String, Tuple2<ActorKey, String>> infosByMovieId){
            return infosByMovieId.mapToPair(l -> new Tuple2<>(l._2._1, l._2._2))
                    .aggregateByKey(new HashSet<String>(),
                        (hs, movie) -> {hs.add(movie); return hs;},
                        (hs1, hs2) -> {hs1.addAll(hs2); return hs1;})
                    .mapValues(HashSet::size)
                    .takeOrdered(10, new SizeComparator());
    }


    public static List<Tuple2<ActorKey, Integer>> top10_v2(JavaPairRDD<String, Tuple2<ActorKey, String>> infosByMovieId){
        return infosByMovieId.mapToPair(l -> new Tuple2<>(l._2._1, l._2._2))
                .groupByKey()
                .mapValues(t -> {
                    List<String> l = StreamSupport.stream(t.spliterator(), true).distinct().collect(Collectors.toList());
                    return l.size();
                })
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

    public void ratings_v1(JavaPairRDD<String, Tuple2<Float, Integer>> old_ratings){
        sc.textFile(ex1_output_dir + "*")
            .map(l -> l.split("\t+"))
            .mapToPair(l -> new Tuple2<>(l[0], new Tuple2<>(Float.parseFloat(l[1]), 1)))
            .reduceByKey((t1, t2) -> new Tuple2<>(t1._1 + t2._1, t1._2 + t2._2))
            .rightOuterJoin(old_ratings)
            .map(p -> {
                Tuple2<Float, Integer> old_values = p._2._2;
                if (p._2._1.isPresent()){
                    Tuple2<Float, Integer> new_values = p._2._1.get();
                    float old_sum = old_values._1 * old_values._2;
                    int new_count = old_values._2 + new_values._2;
                    double new_med = Math.round((old_sum + new_values._1) / new_count * 10) / 10.0;
                    return p._1 + "\t" + new_med + "\t" + new_count;
                }
                else
                    return p._1 + "\t" + old_values._1 + "\t" + old_values._2;
            })
            .coalesce(1)
            .saveAsTextFile(ex2_output_dir + "2c_tmp");
    }

    public static void main(String[] args) throws IOException, URISyntaxException {
        SparkConf conf = new SparkConf().setAppName("Exerciocio 2");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Ex2 e = new Ex2(sc);


        // (ActorId, ActorName)
        JavaPairRDD<String, String> actorsNames = e.genericMapOperation("hdfs://namenode:9000/input/name.basics.tsv",
                l -> !l[0].equals("nconst"),
                l -> new Tuple2<>(l[0], l[1]));


        // (ActorId, MovieId)
        JavaPairRDD<String, String> movieIdByActorId = e.genericMapOperation("hdfs://namenode:9000/input/title.principals.tsv",
                l -> !l[0].equals("tconst") && (l[3].equals("actor") || l[3].contains("actress") || l[3].contains("self")),
                l -> new Tuple2<>(l[2], l[0]));


        // (MovieId, MovieName)
        JavaPairRDD<String, String> moviesTitles = e.genericMapOperation("hdfs://namenode:9000/input/title.basics.tsv",
                l -> !l[0].equals("tconst"),
                l -> new Tuple2<>(l[0], l[2]));


        // (MovieId, MovieRating)
        JavaPairRDD<String, Tuple2<Float, Integer>> old_moviesRating = e.genericMapOperation("hdfs://namenode:9000/input/title.ratings.tsv",
                l -> !l[0].equals("tconst"),
                l -> new Tuple2<>(l[0], new Tuple2<>(Float.parseFloat(l[1]), Integer.parseInt(l[2]))))
                .cache();


        // (MovieId, ActorKey)
        // resultado em cache para usar no top3 e collabs
        JavaPairRDD<String, Tuple2<ActorKey, String>> infosByMovieId = actorsNames.join(movieIdByActorId)
                .mapToPair(l -> new Tuple2<>(l._2._2, new ActorKey(l._2._1, l._1)))
                .join(moviesTitles)
                .cache();

/*
        Timer t = new Timer(TimeUnit.SECONDS);
        t.start();
        System.out.println("Top 10: V2" + top10_v1(infosByMovieId).toString());
        t.addCheckpoint("Top 10 time: ");
        collaborators_v1(infosByMovieId).saveAsTextFile(ex2_output_dir + "/collabs");
        t.addCheckpoint("Collabs time");
        t.print();

 */
        e.ratings_v1(old_moviesRating);
        merge(ex2_c_tmp_output_dir, ex2_c_output_dir);
        //TODO apagar diretoria tmp
    }
}
