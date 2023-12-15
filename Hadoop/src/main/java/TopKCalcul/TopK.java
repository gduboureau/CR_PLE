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

    public static class TopKMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private Map<String, TreeMap<DeckValue, Double>> mapWeek; 
        private Map<String, TreeMap<DeckValue, Double>> mapMonth;
        private Map<String, TreeMap<DeckValue, Double>> mapGlobal;
        private int k;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            mapWeek = new HashMap<String, TreeMap<DeckValue, Double>>();
            mapMonth = new HashMap<String, TreeMap<DeckValue, Double>>();
            mapGlobal = new HashMap<String, TreeMap<DeckValue, Double>>();
            this.k = context.getConfiguration().getInt("k", 10);
        }

        private void processEntry(String type, String valueType, DeckValue deckValue, Double value, Map<String, TreeMap<DeckValue, Double>> map){
            if (!map.containsKey(valueType)) {
                map.put(valueType, new TreeMap<DeckValue, Double>(new KeyComparator()));
            }
            map.get(valueType).put(deckValue, value);
            if (map.get(valueType).size() > k) {
                map.get(valueType).remove(map.get(valueType).firstKey());
            }
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {

                String line = value.toString();
                String[] tokens = line.split("\t");
                String id = tokens[0];
                String[] stats = tokens[1].split(",");

                String[] type = id.split("_");
                String[] nameStats = {"useDeck", "bestClan", "diffForceWin", "winDeck", "nbPlayers"};

                DeckValue deckValue = null;

                for (int i = 0; i < stats.length; i++) {
                    String newLine = id + "_" + nameStats[i];
                    String valueType = type[1] + "_" + nameStats[i];
                    if (type[0].equals("WEEK")) {
                        deckValue = new DeckValue(type[2], Double.parseDouble(stats[i]), newLine);
                        processEntry("WEEK", valueType , deckValue, Double.parseDouble(stats[i]), mapWeek);
                    }else if (type[0].equals("MONTH")) {
                        deckValue = new DeckValue(type[2], Double.parseDouble(stats[i]), newLine);
                        processEntry("MONTH", valueType, deckValue, Double.parseDouble( stats[i]), mapMonth);
                    } else if (type[0].equals("GLOBAL")) {
                        deckValue = new DeckValue(type[1], Double.parseDouble(stats[i]), newLine);
                        processEntry("GLOBAL", nameStats[i], deckValue, Double.parseDouble(stats[i]), mapGlobal);
                    }
                }


            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void writeDescendingKey(Map<String, TreeMap<DeckValue, Double>> map, Context context) throws IOException, InterruptedException {
            for (String type : map.keySet()) {
                for (DeckValue key : map.get(type).descendingKeySet()) {
                    context.write(new Text(key.getLine()), new DoubleWritable(key.getValue()));
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            writeDescendingKey(mapWeek, context);
            writeDescendingKey(mapMonth, context);
            writeDescendingKey(mapGlobal, context);
        }
    }

    public static class TopKReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private Map<String, TreeMap<DeckValue, Double>> mapWeek;
        private Map<String, TreeMap<DeckValue, Double>> mapMonth;
        private Map<String, TreeMap<DeckValue, Double>> mapGlobal;
        private int k;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            mapWeek = new HashMap<String, TreeMap<DeckValue, Double>>();
            mapMonth = new HashMap<String, TreeMap<DeckValue, Double>>();
            mapGlobal = new HashMap<String, TreeMap<DeckValue, Double>>();
            this.k = context.getConfiguration().getInt("k", 10);
        }

        private void processMapData(Map<String, TreeMap<DeckValue, Double>> mapData, String type, DeckValue deckValue, double max) {
            if (!mapData.containsKey(type)) {
                mapData.put(type, new TreeMap<DeckValue, Double>(new KeyComparator()));
            }
    
            mapData.get(type).put(deckValue, max);
            if (mapData.get(type).size() > k) {
                mapData.get(type).remove(mapData.get(type).firstKey());
            }
        }

        public void reduce(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {
            
            String line = key.toString();
            String[] tokens = line.split("_");
            String[] nameStats = {"useDeck", "bestClan", "diffForceWin", "winDeck", "nbPlayers"};
            DeckValue deckValue = null;

            for (int i = 0; i < nameStats.length; i++) {
                String valueType = tokens[1] + "_" + nameStats[i];
                if (tokens[0].equals("WEEK")) {
                    deckValue = new DeckValue(tokens[2], value.get(), line);
                    processMapData(mapWeek, valueType, deckValue, value.get());
                }else if (tokens[0].equals("MONTH")) {
                    deckValue = new DeckValue(tokens[2], value.get(), line);
                    processMapData(mapMonth, valueType, deckValue, value.get());
                } else if (tokens[0].equals("GLOBAL")) {
                    deckValue = new DeckValue(tokens[1], value.get(), line);
                    processMapData(mapGlobal, nameStats[i], deckValue, value.get());
                }
            }
        }

        public void writeDescendingKey(Map<String, TreeMap<DeckValue, Double>> map, Context context) throws IOException, InterruptedException {
            for (String type : map.keySet()) {
                for (DeckValue key : map.get(type).descendingKeySet()) {
                    context.write(new Text(key.getLine()), new DoubleWritable(key.getValue()));
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            writeDescendingKey(mapWeek, context);
            writeDescendingKey(mapMonth, context);
            writeDescendingKey(mapGlobal, context);
        }
    }

    public static void mainTopK(String input, String output, int k) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("k", k);
        Job job = Job.getInstance(conf, "TopKWeeks");
        job.setNumReduceTasks(1);
        job.setJarByClass(TopK.class);
        job.setMapperClass(TopKMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setReducerClass(TopKReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
    }
}
