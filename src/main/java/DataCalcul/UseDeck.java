package DataCalcul;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jettison.json.JSONObject;


public class UseDeck {

    public static class UseDeckMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

        private final static DoubleWritable one = new DoubleWritable(1.0);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            try {
                JSONObject obj = new JSONObject(value.toString());

                if (obj.has("cards") && obj.has("cards2") && obj.has("date")){

                    String cards1 = obj.getString("cards");
                    String cards2 = obj.getString("cards2");
                    String date = obj.getString("date");

                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                    Date parsingDate = dateFormat.parse(date);
                    Calendar calendar = Calendar.getInstance();
                    calendar.setTime(parsingDate);
                    int week = calendar.get(Calendar.WEEK_OF_YEAR);
                    int month = calendar.get(Calendar.MONTH) + 1; // month starts from 0

                    word.set("WEEK_" + week + "_" + cards1);
                    context.write(word, one);

                    word.set("MONTH_" + month + "_" + cards1);
                    context.write(word, one);

                    word.set("GLOBAL_" + cards1);
                    context.write(word, one);

                    word.set("WEEK_" + week + "_" + cards2);
                    context.write(word, one);

                    word.set("MONTH_" + month + "_" + cards2);
                    context.write(word, one);

                    word.set("GLOBAL_" + cards2);
                    context.write(word, one);

                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        }

    }

    public static class UseDeckReducer extends Reducer<Text, DoubleWritable, Text, IntWritable> {

        Text word = new Text();
    
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
    
            for (DoubleWritable value : values) {
                sum += value.get();
            }
    
            word.set(key.toString() + "," + sum);
            context.write(word, null);
        }

    }


    public static void JobUseDeck(String[] args, String output) throws Exception {
    
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "UseDeck");

        job.setJarByClass(UseDeck.class);
        job.setMapperClass(UseDeckMapper.class);
        job.setReducerClass(UseDeckReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(output));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
    
}
