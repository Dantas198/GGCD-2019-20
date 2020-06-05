/**
 * Este codigo resolve a questão 2.C:
 * Ratings:Atualize o ficheirotitle.ratings.tsvtendo em conta o seu conteúdo ante-rior e os novos votos recebidos até ao momento.
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class NewRatings {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("simplestream");
        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaPairRDD<String, Tuple2<Double, Integer>> old_rattings = sc.textFile("hdfs://namenode:9000/input/title.ratings.tsv")
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("tconst"))
                .mapToPair(l -> new Tuple2<>(l[0], new Tuple2<>(Double.parseDouble(l[1]), Integer.parseInt(l[2]))));

        JavaRDD<Tuple2<String, Tuple2<Double, Integer>>> new_rattings = sc.textFile("ex1Output/*")
                .map(l -> {
                    String[] parts = l.split(",");
                    parts[0] = parts[0].substring(2);
                    parts[1] = parts[1].substring(0, parts[1].length()-1);
                    return parts;
                })
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

                        Tuple2<Double, Integer> tmp_pair = new Tuple2<>(current_ratting, current_numVotes);

                        return new Tuple2<>(p._1, tmp_pair);
                    }
                    else{
                        return new Tuple2<>(p._1, old_pairs);
                    }
                })
                .sortBy(new Function<Tuple2<String, Tuple2<Double, Integer>>, Object>() {
                    @Override
                    public Object call(Tuple2<String, Tuple2<Double, Integer>> stringTuple2Tuple2) throws Exception {
                        return stringTuple2Tuple2._1;
                    }
                }, true, 1);

        new_rattings.saveAsTextFile("title.ratings.tsv");
    }
}
