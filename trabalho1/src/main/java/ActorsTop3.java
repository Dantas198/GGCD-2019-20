import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import utils.TextFloatPair;
import utils.TextPair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


public class ActorsTop3 {
    public static class JoinTitleMapper extends TableMapper<Text,TextPair> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            byte[] name = value.getValue(Bytes.toBytes("info"), Bytes.toBytes("primaryTitle"));
            context.write(new Text(value.getRow()), new TextPair(new Text("left"), new Text(name)));
        }
    }

    public static class JoinRatingMapper extends Mapper<LongWritable, Text, Text, TextPair> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() !=  0) {
                String[] fields = value.toString().split("\\t");
                context.write(new Text(fields[0]), new TextPair("right", fields[1]));
            }
        }
    }

    public static class JoinMovieInfoReducer extends Reducer<Text, TextPair, Text, TextPair> {
        @Override
        protected void reduce(Text key, Iterable<TextPair> values, Context context) throws IOException, InterruptedException {
            Text name = new Text("");
            // leftOuterJoin
            Text rating = new Text("0.0");
            //Lista tem sempre 2 ou menos elementos;
            for(TextPair tp : values){
                if(tp.getFirst().toString().equals("left"))
                    name = new Text(tp.getSecond());
                else
                    rating = new Text(tp.getSecond());
            }
            context.write(key, new TextPair(name, rating));
        }
    }

    public static class JoinActorMap extends Mapper<LongWritable, Text, Text, TextPair> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() !=  0){
                String[] words = value.toString().split("\\t");
                //System.out.println(Arrays.toString(words));
                if(words[3].equals("actor") || words[3].equals("actress") || words[3].equals("self"))
                    context.write(new Text(words[0]), new TextPair(new Text("left"), new Text(words[2])));
            }
        }
    }

    public static class JoinMovieInfoMap extends Mapper<LongWritable, Text, Text, TextPair> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\\t");
            context.write(new Text(fields[0]), new TextPair(new Text("right"), new Text(fields[1]+"\t"+fields[2])));
        }
    }

    // TODO compare to V2
    public static class JoinReducerForActorV1 extends Reducer<Text, TextPair, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<TextPair> values, Context context) throws IOException, InterruptedException {
            Text movieInfo = null;
            List<Text> actors = new ArrayList<>();
            for(TextPair tp : values){
                if(tp.getFirst().toString().equals("left"))
                    actors.add(new Text(tp.getSecond().toString()));
                else {
                    movieInfo = new Text(key+"\t"+tp.getSecond());
                }
            }
            if(movieInfo != null) {
                for(Text actor : actors)
                    context.write(actor, movieInfo);
            }
        }
    }
    // ------ top 3 with secundary sort ------

    public static class FirstPartitioner extends Partitioner<TextFloatPair, Text> {
        @Override
        public int getPartition(TextFloatPair key, Text value, int numPartitions) {
            return (key.getFirst().hashCode() * 127) % numPartitions;
        }
    }


    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(TextFloatPair.class, true);
        }
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            TextFloatPair tfp1 = (TextFloatPair) w1;
            TextFloatPair tfp2 = (TextFloatPair) w2;
            return tfp1.compareTo(tfp2);
        }
    }

    public static class GroupComparator extends WritableComparator {
        protected GroupComparator() {
            super(TextFloatPair.class, true);
        }
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            TextFloatPair tfp1 = (TextFloatPair) w1;
            TextFloatPair tfp2 = (TextFloatPair) w2;
            return tfp1.first.compareTo(tfp2.first);
        }
    }


    public static class MyMapperTop3SS extends Mapper<LongWritable, Text, TextFloatPair, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\\t");
            System.out.println(Arrays.toString(fields));
            context.write(new TextFloatPair(new Text(fields[0]), new FloatWritable(Float.parseFloat(fields[3]))), new Text(fields[2]));
        }
    }

    // if we want the rating then Text -> TextFloatPair
    public static class MyReducerTop3SS extends TableReducer<TextFloatPair, Text, ImmutableBytesWritable>{
        @Override
        protected void reduce(TextFloatPair key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            byte[] row = Bytes.toBytes(key.first.toString());
            Put put = new Put(row);
            int i = 0;
            for(Text movie : value){
                if(i<3)
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("r" + i + 1), Bytes.toBytes(movie.toString()));
                i++;
            }
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("c"),Bytes.toBytes(i));
            context.write(null, put);
        }
    }

    // ----- top 3 with local sort -----------


    public static class MyMapperTop3 extends Mapper<LongWritable, Text, Text, TextFloatPair>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\\t");
            System.out.println(Arrays.toString(fields));
            context.write(new Text(fields[0]), new TextFloatPair(new Text(fields[2]), new FloatWritable(Float.parseFloat(fields[3]))));
        }
    }

    public static class MyReducerTop3 extends TableReducer<Text, TextFloatPair, ImmutableBytesWritable>{
        @Override
        protected void reduce(Text key, Iterable<TextFloatPair> values, Context context) throws IOException, InterruptedException {
            Stream<TextFloatPair> s = StreamSupport.stream(values.spliterator(), true)
                    .sorted((a, b) -> {
                        FloatWritable x = a.second;
                        FloatWritable y = b.second;
                        return Float.compare(y.get(), x.get());
                    });
            List<TextFloatPair> list = s.collect(Collectors.toList());
            byte[] row = Bytes.toBytes(key.toString());
            Put put = new Put(row);
            int size = list.size();
            if(size > 3)
                list = new ArrayList<>(list.subList(0, 2));

            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("c"),Bytes.toBytes(size));
            int i = 1;
            for(TextFloatPair p : list){
                if(i == 1 && Float.compare(0, p.second.get()) == 0)
                    break;
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("r" + i), Bytes.toBytes(p.first + "%" + p.second));
                i++;
            }
            context.write(null, put);
        }
    }

    public static void executeMovieInfoJob(Configuration conf, String tableName, String fileName, String output) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(conf, "movie_info_join");
        job.setJarByClass(ActorsTop3.class);

        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
        scan.addFamily(Bytes.toBytes("info"));

        TableMapReduceUtil.initTableMapperJob(tableName, scan, JoinTitleMapper.class, Text.class, TextPair.class, job);

        MultipleInputs.addInputPath(job, new Path("none"),
                TableInputFormat.class, JoinTitleMapper.class);
        MultipleInputs.addInputPath(job, new Path(fileName),
                TextInputFormat.class, JoinRatingMapper.class);

        job.setReducerClass(JoinMovieInfoReducer.class);

        //job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(utils.TextPair.class);

        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }

    public static void executeActorWithMovieInfoJob(Configuration conf, String infoFile, String actorFile, String output) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(conf, "movie_info_actor_join");

        job.setJarByClass(ActorsTop3.class);
        MultipleInputs.addInputPath(job, new Path(infoFile),
                TextInputFormat.class, JoinMovieInfoMap.class);
        MultipleInputs.addInputPath(job, new Path(actorFile),
                TextInputFormat.class, JoinActorMap.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TextPair.class);

        job.setReducerClass(JoinReducerForActorV1.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TextPair.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
    }

    public static void executeTop3Job(Configuration conf, String name, String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(conf, name);
        job.setJarByClass(ActorsTop3.class);
        job.setMapperClass(MyMapperTop3.class);
        job.setReducerClass(MyReducerTop3.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TextFloatPair.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, input);

        TableMapReduceUtil.initTableReducerJob(output, MyReducerTop3.class, job);
        job.setNumReduceTasks(1);

        job.waitForCompletion(true);
    }


}
