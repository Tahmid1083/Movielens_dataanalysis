import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageRating {

    public static class RatingMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
        private Text movieId = new Text();
        private FloatWritable rating = new FloatWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("userId")) return; // Skip CSV header
            String[] tokens = line.split(",");
            if (tokens.length >= 3) {
                movieId.set(tokens[1]);
                rating.set(Float.parseFloat(tokens[2]));
                context.write(movieId, rating);
            }
        }
    }

    public static class RatingReducer extends Reducer<Text, FloatWritable, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;
            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }
            float avg = sum / count;
            context.write(key, new Text("AvgRating: " + avg + "\tTotalCount: " + count));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Rating Per Movie");
        job.setJar("MapReduceJobs.jar");
        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(RatingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}