import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TopK {

    public static class TopKWeeksMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String line = value.toString();
                String[] tokens = line.split(",");
                String winValue = tokens[1];
                context.write(new Text(tokens[0]), new IntWritable(Integer.parseInt(winValue)));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class TopKWeeksReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private Map<String, TreeMap<Integer, String>> mapWeek;
        private Map<String, TreeMap<Integer, String>> mapMonth;
        private TreeMap<Integer, String> mapGlobal;
        private int k = 10;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            mapWeek = new HashMap<String, TreeMap<Integer, String>>();
            mapMonth = new HashMap<String, TreeMap<Integer, String>>();
            mapGlobal = new TreeMap<Integer, String>();
            this.k = context.getConfiguration().getInt("k", 10);
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String line = key.toString();
            String[] tokens = line.split("_");
            if (tokens[0].equals("WEEK")){
                int max = 0;
                for (IntWritable value : values) {
                    max = Math.max(max, value.get());
                }
                String week = tokens[1];
                if (!mapWeek.containsKey(week)) {
                    mapWeek.put(week, new TreeMap<Integer, String>());
                }
                mapWeek.get(week).put(max, line);
                if (mapWeek.get(week).size() > k) {
                    mapWeek.get(week).remove(mapWeek.get(week).firstKey());
                }
            }else if (tokens[0].equals("MONTH")){
                int max = 0;
                for (IntWritable value : values) {
                    max = Math.max(max, value.get());
                }
                String month = tokens[1];
                if (!mapMonth.containsKey(month)) {
                    mapMonth.put(month, new TreeMap<Integer, String>());
                }
                mapMonth.get(month).put(max, line);
                if (mapMonth.get(month).size() > k) {
                    mapMonth.get(month).remove(mapMonth.get(month).firstKey());
                }
            }else if (tokens[0].equals("GLOBAL")){
                int max = 0;
                for (IntWritable value : values) {
                    max = Math.max(max, value.get());
                }
                mapGlobal.put(max, line);
                if (mapGlobal.size() > k) {
                    mapGlobal.remove(mapGlobal.firstKey());
                }
            }
            
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (String week : mapWeek.keySet()) {
                for (Integer winValue : mapWeek.get(week).descendingKeySet()) {
                    context.write(new Text(mapWeek.get(week).get(winValue)), new IntWritable(winValue));
                }
            }
            for (String month : mapMonth.keySet()) {
                for (Integer winValue : mapMonth.get(month).descendingKeySet()) {
                    context.write(new Text(mapMonth.get(month).get(winValue)), new IntWritable(winValue));
                }
            }
            for (Integer winValue : mapGlobal.descendingKeySet()) {
                context.write(new Text(mapGlobal.get(winValue)), new IntWritable(winValue));
            }
        }
    }

    public static void mainTopKWeeks(String input, String output) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("k", 10);
        Job job = Job.getInstance(conf, "TopKWeeks");
        job.setNumReduceTasks(1);
        job.setJarByClass(TopK.class);
        job.setMapperClass(TopKWeeksMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(TopKWeeksReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
