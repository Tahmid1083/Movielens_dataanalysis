import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserRatingJoin {

    public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Map<String, String> userMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                // FIXED LINE: Extract only the simple file name (e.g. "users.csv")
                String fileName = new Path(cacheFiles[0].getPath()).getName();
                File cacheFile = new File(fileName);
                
                try (BufferedReader reader = new BufferedReader(new FileReader(cacheFile))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.startsWith("userId")) continue;
                        String[] tokens = line.split(",");
                        if (tokens.length >= 3) {
                            userMap.put(tokens[0], tokens[1] + "," + tokens[2]);
                        }
                    }
                }
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("userId")) return;
            String[] tokens = line.split(",");
            if (tokens.length >= 3) {
                String userId = tokens[0];
                String movieId = tokens[1];
                String rating = tokens[2];
                
                String userInfo = userMap.getOrDefault(userId, "Unknown,Unknown");
                context.write(new Text(userId), new Text("MovieID:" + movieId + " | Rating:" + rating + " | Demographics(Gender,Age):" + userInfo));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Map Side Join Users and Ratings");
        job.setJar("MapReduceJobs.jar");
        job.setMapperClass(JoinMapper.class);
        job.setNumReduceTasks(0);
        
        // Pass HDFS path to DistributedCache
        job.addCacheFile(new URI(args[0]));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}