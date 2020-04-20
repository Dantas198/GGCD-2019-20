import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


public class Main {
    JavaSparkContext sc;

    public Main(JavaSparkContext sc){
        this.sc = sc;
    }

    // função que retorna um JavaPairRDD genérico aplicável a muitos casos básicos de mapeamento dos ficheiros originais
    public <K, V> JavaPairRDD<K, V> genericMapOperation(String inputFile, Function<String[],Boolean> f1, PairFunction<String[],K,V> f2){
        return sc.textFile(inputFile)
                .map(l -> l.split("\\t"))
                .filter(f1)
                .mapToPair(f2);
    }

    // JavaPairRDD para o cálculo do top3. Adicionalmente cria o objeto Actor que também contém o número de filmes total em que participou
    // @param infosByMovieID - RDD que contém um mapeamento de ID de filme para um tuplo de informação do ator (obtido do title.principals)
    // e informação adicional do filme
    public JavaPairRDD<Actor, List<MovieInfo>> top3Movies(JavaPairRDD<String, Tuple2<ActorKey, MovieInfo>> infosByMovieId){
        // Chave passa a ser ActorKey
        return infosByMovieId.mapToPair(l -> new Tuple2<>(l._2._1, l._2._2))
                .groupByKey()
                .mapToPair(l -> {
                    Stream<MovieInfo> s = StreamSupport.stream(l._2.spliterator(), true)
                            .sorted(Comparator.comparing(MovieInfo::getRating).reversed());
                    List<MovieInfo> list = s.collect(Collectors.toList());
                    int size = list.size();
                    if(size < 3)
                        return new Tuple2<>(new Actor(l._1, size), list);
                    else
                        return new Tuple2<>(new Actor(l._1, size), new ArrayList<>(list.subList(0, 2)));
                })
                .cache();
    }

    // JavaPairRDD para o cálculo dos colaboradores
    public JavaPairRDD<ActorKey, Iterable<Tuple2<ActorKey, MovieInfo>>> collaborators(JavaPairRDD<String, Tuple2<ActorKey, MovieInfo>> infosByMovieId){
        return infosByMovieId.groupByKey()
                .flatMapToPair(l -> {
                    List<Tuple2<ActorKey, MovieInfo>> list = StreamSupport.stream(l._2.spliterator(), true).collect(Collectors.toList());
                    List<Tuple2<ActorKey, Tuple2<ActorKey, MovieInfo>>> result = new ArrayList<>();
                    for(int i=0; i<list.size(); i++)
                        for(int j = 0; j<list.size(); j++)
                            if(i!=j)
                                result.add(new Tuple2<>(list.get(i)._1, list.get(j)));
                    return result.iterator();
                })
                .groupByKey();
    }

    // JavaPairRDD para a ordenação de atores por contagem de filmes em que participou cada um
    public JavaPairRDD<String, Integer> sortActorsByMovieCount(JavaPairRDD<Actor, List<MovieInfo>> moviesByActor){
        return moviesByActor.sortByKey(false)
                .mapToPair(p -> new Tuple2<>(p._1.actorKey.actorName, p._1.numMovies))
                .cache();
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Exerciocio 1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Main m = new Main(sc);

        // (ActorId, ActorName)
        JavaPairRDD<String, String> actorsNames = m.genericMapOperation("hdfs://namenode:9000/input/name.basics.tsv",
                l -> !l[0].equals("nconst"),
                l -> new Tuple2<>(l[0], l[1]));


        // (ActorId, MovieId)
        JavaPairRDD<String, String> movieIdByActorId = m.genericMapOperation("hdfs://namenode:9000/input/title.principals.tsv",
                l -> !l[0].equals("tconst") && (l[3].equals("actor") || l[3].contains("actress") || l[3].contains("self")),
                l -> new Tuple2<>(l[2], l[0]));


        // (MovieId, MovieName)
        JavaPairRDD<String, String> moviesTitles = m.genericMapOperation("hdfs://namenode:9000/input/title.basics.tsv",
                l -> !l[0].equals("tconst"),
                l -> new Tuple2<>(l[0], l[2]));


        // (MovieId, MovieRating)
        JavaPairRDD<String, Float> moviesRating = m.genericMapOperation("hdfs://namenode:9000/input/title.ratings.tsv",
                l -> !l[0].equals("tconst"),
                l -> new Tuple2<>(l[0], Float.parseFloat(l[1])));


        // (MovieId, MovieInfo) with leftOuterJoin to include missing ratings values so that it functions
        // properly with movieCount on top3
        JavaPairRDD<String, MovieInfo> moviesInfo = moviesTitles.leftOuterJoin(moviesRating)
                .mapToPair(l -> new Tuple2<>(l._1, new MovieInfo(l._2._1, l._2._2)));


        // (MovieId, ActorKey)
        // resultado em cache para usar no top3 e collabs
        JavaPairRDD<String, Tuple2<ActorKey, MovieInfo>> infosByMovieId = actorsNames.join(movieIdByActorId)
                .mapToPair(l -> new Tuple2<>(l._2._2, new ActorKey(l._2._1, l._1)))
                .join(moviesInfo)
                .cache();


        JavaPairRDD<Actor, List<MovieInfo>> moviesByActor = m.top3Movies(infosByMovieId);
        List<Tuple2<Actor, List<MovieInfo>>> moviesByActorList = m.top3Movies(infosByMovieId).collect();

        List<Tuple2<ActorKey, Iterable<Tuple2<ActorKey, MovieInfo>>>> collaborators = m.collaborators(infosByMovieId).collect();

        JavaPairRDD<String, Integer> moviesCountByActor = m.sortActorsByMovieCount(moviesByActor);


        //Lista de ator para o seu número de filmes
        System.out.println(moviesCountByActor.collect().toString());

        //top 10
        System.out.println(moviesCountByActor.take(10).toString());

        //top3 por ator
        moviesByActorList.forEach(v -> {
            System.out.println("Actor: " + v._1.actorKey.actorName);
            System.out.println("Top 3: ");
            v._2.forEach(info -> System.out.println("-> " + info.title + " with rating = " + info.rating));
            System.out.println();
        });

        //collabs
        collaborators.forEach(v -> {
            System.out.println("\nActor: " + v._1.actorName + " collab list: ");
            v._2.forEach(collab -> {
                System.out.println("Collab: " + collab._1.actorName + " on movie " + collab._2.title);
            });
        });

    }
}
