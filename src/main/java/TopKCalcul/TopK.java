package TopKCalcul;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

    public static class TopKWeeksMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private Map<String, TreeMap<String, Double>> mapWeek; 
        private Map<String, TreeMap<String, Double>> mapMonth;
        private TreeMap<String, Double> mapGlobal;
        private int k;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            mapWeek = new HashMap<String, TreeMap<String, Double>>();
            mapMonth = new HashMap<String, TreeMap<String, Double>>();
            mapGlobal = new TreeMap<String, Double>(new KeyComparator());
            this.k = context.getConfiguration().getInt("k", 10);
        }

        private void processEntry(String type, String valueType, String line, Double value, Map<String, TreeMap<String, Double>> map){
            if (!map.containsKey(valueType)) {
                map.put(valueType, new TreeMap<String, Double>(new KeyComparator()));
            }
            map.get(valueType).put(line, value);
            if (map.get(valueType).size() > k) {
                map.get(valueType).remove(map.get(valueType).firstKey());
            }
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String line = value.toString();
                String[] tokens = line.split(",");
                String winValue = tokens[1];
                String[] type = tokens[0].split("_");

                if (type[0].equals("WEEK")) {
                    processEntry("WEEK", type[1], line, Double.parseDouble(winValue), mapWeek);
                } else if (type[0].equals("MONTH")) {
                    processEntry("MONTH", type[1], line, Double.parseDouble(winValue), mapMonth);
                } else if (type[0].equals("GLOBAL")) {
                    mapGlobal.put(line, Double.parseDouble(winValue));
                    if (mapGlobal.size() > k) {
                        mapGlobal.remove(mapGlobal.firstKey());
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void writeDescendingKey(Map<String, TreeMap<String, Double>> map, Context context) throws IOException, InterruptedException {
            for (String type : map.keySet()) {
                for (String key : map.get(type).descendingKeySet()) {
                    context.write(new Text(key), new DoubleWritable(map.get(type).get(key)));
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            writeDescendingKey(mapWeek, context);
            writeDescendingKey(mapMonth, context);
            for (String key : mapGlobal.descendingKeySet()) {
                context.write(new Text(key), new DoubleWritable(mapGlobal.get(key)));
            }
        }
    }

    public static class TopKWeeksReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private Map<String, TreeMap<String, Double>> mapWeek;
        private Map<String, TreeMap<String, Double>> mapMonth;
        private TreeMap<String, Double> mapGlobal;
        private int k;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            mapWeek = new HashMap<String, TreeMap<String, Double>>();
            mapMonth = new HashMap<String, TreeMap<String, Double>>();
            mapGlobal = new TreeMap<String, Double>(new KeyComparator());
            this.k = context.getConfiguration().getInt("k", 10);
        }

        private void processMapData(Map<String, TreeMap<String, Double>> mapData, String type, String line, double max) {
            if (!mapData.containsKey(type)) {
                mapData.put(type, new TreeMap<String, Double>(new KeyComparator()));
            }
    
            mapData.get(type).put(line, max);
            if (mapData.get(type).size() > k) {
                mapData.get(type).remove((Object) mapData.get(type).firstKey());
            }
        }

        private double getMaxValue(Iterable<DoubleWritable> values) {
            double max = 0;
            for (DoubleWritable value : values) {
                max = Math.max(max, value.get());
            }
            return max;
        }

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            
            String line = key.toString();
            String[] tokens = line.split("_");
            double max = getMaxValue(values);

            if (tokens[0].equals("WEEK")){
                processMapData(mapWeek, tokens[1], line, max);
            }else if (tokens[0].equals("MONTH")){
                processMapData(mapMonth, tokens[1], line, max);
            }else if (tokens[0].equals("GLOBAL")){
                mapGlobal.put(line, max);
                if (mapGlobal.size() > k) {
                    mapGlobal.remove(mapGlobal.firstKey());
                }
            }
        }

        public void writeDescendingKey(Map<String, TreeMap<String, Double>> map, Context context) throws IOException, InterruptedException {
            for (String type : map.keySet()) {
                for (String key : map.get(type).descendingKeySet()) {
                    String[] tokens = key.split(",");
                    context.write(new Text(tokens[0]), new DoubleWritable(map.get(type).get(key)));
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            writeDescendingKey(mapWeek, context);
            writeDescendingKey(mapMonth, context);
            for (String key : mapGlobal.descendingKeySet()) {
                String tokens[] = key.split(",");
                context.write(new Text(tokens[0]), new DoubleWritable(mapGlobal.get(key)));
            }
        }
    }

    public static void mainTopKWeeks(String input, String output, int k) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("k", k);
        Job job = Job.getInstance(conf, "TopKWeeks");
        job.setNumReduceTasks(1);
        job.setJarByClass(TopK.class);
        job.setMapperClass(TopKWeeksMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setReducerClass(TopKWeeksReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
    }
}
