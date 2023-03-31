import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherData {
    public static class Map
            extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split("\\s+");
            String date = tokens[1];
            float tempMax = Float.parseFloat(tokens[6].trim());
            float tempMin = Float.parseFloat(tokens[7].trim());

            if(tempMax > 40.0) {
                context.write(new Text("Hot Day " + date), new Text(String.valueOf(tempMax)));
            }
            if(tempMin < 10.0) {
                context.write(new Text("Cold Day " + date), new Text(String.valueOf(tempMin)));
            }

        }
    }

    public static class Reduce
            extends Reducer<Text,Text,Text,Text>{

        public void reduce(Text key, Iterator<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            String temperature = values.next().toString();
            context.write(key, new Text(temperature));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "weather");
        job.setJarByClass(WeatherData.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
