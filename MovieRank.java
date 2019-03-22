import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.HashMap;

public class MovieRank {

    // Mapper class used by job1, for joining movies.csv with reviews.csv
    public static class MapForJoining extends Mapper<LongWritable, Text, IntWritable, Text> {

        HashMap<Integer, String> movieInfo = new HashMap<Integer, String>();

        // setup method for loading smaller file movies.csv into local memory Hashmap
        public void setup(Context context) throws IOException {

            Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            BufferedReader input = new BufferedReader(new FileReader(files[0].toString()));
            String line = null;

            while ((line = input.readLine()) != null) {
                String[] fields = line.split(",");

                // ignoring the header in movies.csv
                if (fields[0].equals("movieId"))
                    continue;
                int len = fields.length;
                Integer movieId = Integer.parseInt(fields[0]);
                String title = fields[1];
                for (int i = 2; i < len - 1; i++) {
                    title = title + "," + fields[i];
                }
                movieInfo.put(movieId, title);
            }
            input.close();
        }

        // map function for joining movies.csv loaded to HashMap with the movieId in reviews.csv
        // Takes each record in reviews.csv as input and gives < movieId, (title 1)> key-value pair as output
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

            String line = value.toString();
            String[] columns = line.split(",");

            // ignoring header in reviews.csv
            if (columns[0].equals("userId")) {
                return;
            }

            Integer movieId = Integer.parseInt(columns[1]);
            // fetching title for each movieId
            String title = movieInfo.get(movieId);
            IntWritable outputKey = new IntWritable(movieId);
            Text outputValue = new Text();
            outputValue.set(title + "\t" + "1");

            con.write(outputKey, outputValue);

        }

    }

    // optional combiner function for grouping the K,V pairs (based on key) coming from each mapper.
    // Takes <moviedId, (title ,1)> as input and gives <movieId, (title, count)> as output.
    public static class CombineForGrouping extends Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context con) throws IOException, InterruptedException {

            int sum = 0;
            Text outputValue = new Text();
            for (Text value : values) {
                String line = value.toString();
                String[] parts =  line.split("\t");
                sum = sum + Integer.parseInt(parts[1]);
                outputValue.set(parts[0] + "\t" + sum);
            }
            con.write(key,outputValue);

        }
    }

    // Mapper output is sorted and shuffled based on movieId. Input to the reducer is <movieId, list(title 1,title 1, ...) >
    // Reducer performs the summing and outputs <count,title>
    public static class ReduceForGrouping extends Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context con) throws IOException, InterruptedException {

            int sum = 0;
            Text outputvalue = new Text();
            for (Text value : values) {
                String line = value.toString();
                String[] parts =  line.split("\t");
                sum = sum + Integer.parseInt(parts[1]);
                outputvalue.set(parts[0]);
            }

            con.write(new IntWritable(sum),outputvalue);

        }
    }

    // Mapper class for Job2. MapForSorting just passes <rank, title> input to the output
    public static class MapForSorting extends Mapper<Text, Text, IntWritable, Text> {

        public void map(Text key, Text value, Context con) throws IOException, InterruptedException {

            IntWritable outputKey = new IntWritable();
            int keyValue = Integer.parseInt(key.toString());
            outputKey.set(keyValue);
            con.write(outputKey, value);

        }
    }

    // Mapper output is sorted based on rank and sent to reducer
    // ReduceForSorting converts < K, list(V,V,V) > to < K,V > pairs
    public static class ReduceForSorting extends Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context con) throws IOException, InterruptedException {
            Text outputValue = new Text();
            for (Text value : values) {
                outputValue.set(value);
                con.write(key, outputValue);
            }


        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException, ClassNotFoundException {

        long startTime = new Date().getTime();

        Configuration config = new Configuration();
        String[] files = new GenericOptionsParser(config, args).getRemainingArgs();
        Job job1 = new Job(config, "Map-side Join");

        // adding movies.csv into the distributed cache
        int num_args = files.length;
        int num_reducers_1, num_reducers_2;
        DistributedCache.addCacheFile(new URI(files[num_args-3]), job1.getConfiguration());

        //Setup for first job "Map-side Join"
        job1.setJarByClass(MovieRank.class);
        job1.setMapperClass(MapForJoining.class);
        //job1.setCombinerClass(CombineForGrouping.class);
        job1.setReducerClass(ReduceForGrouping.class);

        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);

        //setup input/output paths for job1
        Path input1 = new Path(files[num_args-2]);
        Path output1 = new Path(files[num_args-1] + "/temp");
        FileInputFormat.addInputPath(job1, input1);
        FileOutputFormat.setOutputPath(job1, output1);

        // setting number of reducers for job1
        if (num_args > 5) {
            num_reducers_1 = Integer.parseInt(files[2]);
            job1.setNumReduceTasks(num_reducers_1);
        }

        job1.waitForCompletion(true);

        //setup configuration for second job "Sorting"
        Configuration config2 = new Configuration();
        Job job2 = new Job(config2, "Sorting");

        job2.setJarByClass(MovieRank.class);
        job2.setMapperClass(MapForSorting.class);
        job2.setReducerClass(ReduceForSorting.class);

        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);

        //setup input/output paths for job2
        Path input2 = new Path(files[num_args-1] + "/temp");
        Path output2 = new Path(files[num_args-1] + "/final");
        FileInputFormat.addInputPath(job2, input2);
        FileOutputFormat.setOutputPath(job2, output2);

        // setting number of reducers for job2
        if (num_args > 4) {
            num_reducers_2 = Integer.parseInt(files[1]);
            job2.setNumReduceTasks(num_reducers_2);
        }
        // printing the time taken to run
        if (job2.waitForCompletion(true)) {
            long endTime = new Date().getTime();
            System.out.println("\n######################\n");
            System.out.println("Total time taken: " + (endTime - startTime) / 1000 + " seconds");
            System.out.println("\n######################\n");
        }

        System.exit(job2.waitForCompletion(true)? 0:1);

    }


}
