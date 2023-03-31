import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CallDataRecord {
    public static class Map
            extends Mapper<Object, Text, Text, LongWritable>{

        Text phoneNumber = new Text();
        LongWritable minutes = new LongWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split("\\|");
            if(tokens[4].equals("1")) {
                phoneNumber.set(tokens[0]);
                minutes.set(calculateTimeInMinutes(tokens[2], tokens[3]));
                context.write(phoneNumber, minutes);
            }
        }

        private long calculateTimeInMinutes(String start, String end) {
            SimpleDateFormat formatter = new SimpleDateFormat(("yyyy-MM-dd HH:mm:ss"));
            long minutes = -1; // if this value happen then there's an error
            try {
                // put code in try catch so that java is not angry
                Date startDate = formatter.parse(start);
                Date endDate = formatter.parse(end);
                long duration = endDate.getTime() - startDate.getTime();
                minutes = duration / (1000 * 60);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return minutes;
        }
    }

    public static class Reduce
            extends Reducer<Text,LongWritable,Text,LongWritable>{

        public void reduce(Text key, Iterable<LongWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            long totalMinutes = 0;
            for(LongWritable val: values) {
                totalMinutes += val.get();
            }
            if(totalMinutes > 60) {
                context.write(key, new LongWritable(totalMinutes));
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "call data record");
        job.setJarByClass(CallDataRecord.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}