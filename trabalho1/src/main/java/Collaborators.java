import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import utils.TextPair;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Collaborators {
/*
    public static class FirstPartitioner extends Partitioner<Actor, > {
        @Override
        public int getPartition(IntPair key, NullWritable value, int numPartitions) {
            // multiply by 127 to perform some mixing
            return Math.abs(key.getFirst() * 127) % numPartitions;
        }
    }

*/
    public static class MovieInfoActorMapper extends Mapper<LongWritable, Text, Text, TextPair> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\\t");
            context.write(new Text(fields[1]), new TextPair(new Text(fields[0]), new Text(fields[2])));
        }
    }

    public static class ActorByActorReducer extends Reducer<Text, TextPair, Text, TextPair> {
        @Override
        protected void reduce(Text key, Iterable<TextPair> values, Context context) throws IOException, InterruptedException {
            List<TextPair> list = StreamSupport.stream(values.spliterator(), true)
                    .collect(Collectors.toList());

            for(int i=0; i<list.size(); i++){
                for(int j = 0; j<list.size(); j++) {
                    if (i != j){
                        context.write(list.get(i).first, list.get(j));
                    }
                }
            }
        }
    }

    public static class CollabsMapper extends Mapper<LongWritable, Text, Text, TextPair> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\\t");
            context.write(new Text(fields[0]), new TextPair(new Text(fields[1]), new Text(fields[2])));
        }
    }

    public static class CollabsReducer extends TableReducer<Text, TextPair, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<TextPair> values, Context context) throws IOException, InterruptedException {
            byte[] row = Bytes.toBytes(key.toString());
            Put put = new Put(row);

            // TODO escrever objeto em ver de string + string
            int i = 1;
            for(TextPair p : values){
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("coll" + i), Bytes.toBytes(p.first + "%" + p.second));
                i++;
            }
            context.write(null, put);
        }
    }
    public static void executeActorByActorJob(Configuration conf, String infoFile, String output) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(conf, "movie_info_actor_join");

        job.setJarByClass(Collaborators.class);
        job.setMapperClass(MovieInfoActorMapper.class);
        job.setReducerClass(ActorByActorReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TextPair.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TextPair.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path(infoFile));
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
    }

    public static void executeCollabsJob(Configuration conf, String name, String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(conf, name);
        job.setJarByClass(Collaborators.class);
        job.setMapperClass(CollabsMapper.class);
        job.setReducerClass(CollabsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TextPair.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, input);

        TableMapReduceUtil.initTableReducerJob(output, CollabsReducer.class, job);
        job.setNumReduceTasks(1);

        job.waitForCompletion(true);
    }
}
