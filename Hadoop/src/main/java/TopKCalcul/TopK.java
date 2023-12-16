package TopKCalcul;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Collections;

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

    private static String[] nameStats = {"useDeck", "bestClan", "diffForceWin", "winDeck", "nbPlayers"};

    private static void processEntry(String[] deckField, Double value, Map<KeyMap, List<DeckDescriptor>> map, int k, String nameStat){
        DeckDescriptor deckValue = null;
        KeyMap keymap = null;
        if (deckField[0].equals("GLOBAL")) {
            deckValue = new DeckDescriptor(deckField[1], value);
            keymap = new KeyMap(deckField[0], nameStat);
        } else {
            deckValue = new DeckDescriptor(deckField[2], value);
            keymap = new KeyMap(deckField[0] + "_" + deckField[1], nameStat);
        }
        
        if (!map.containsKey(keymap)) {
            map.put(keymap, new ArrayList<DeckDescriptor>());
        }
        map.get(keymap).add(deckValue);

        Collections.sort(map.get(keymap)); 

        if (map.get(keymap).size() > k) {
            map.get(keymap).remove(map.get(keymap).size() - 1);
        }
    }

    public static Double getMax(Iterable<DoubleWritable> values) {
        double max = 0;
        for (DoubleWritable value : values) {
            max = Math.max(max, value.get());
        }
        return max;
    }
    

    public static class TopKMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private Map<KeyMap, List<DeckDescriptor>> topKMap; 
        private int k;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            topKMap = new TreeMap<>();
            this.k = context.getConfiguration().getInt("k", 10);
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {

                String line = value.toString();
                String[] tokens = line.split("\t");
                String deck = tokens[0];
                String[] stats = tokens[1].split(",");

                String[] deckField = deck.split("_");


                for (int i = 0; i < stats.length; i++) {
                    processEntry(deckField , Double.parseDouble(stats[i]), topKMap, k, nameStats[i]);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (KeyMap type : topKMap.keySet()) {
                for (DeckDescriptor key : topKMap.get(type)) {
                    context.write(new Text(type + "_" + key.getCards()), new DoubleWritable(key.getValue()));
                }
            }
        }
    }

    public static class TopKReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private Map<KeyMap, List<DeckDescriptor>> topKMap;
        private int k;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            topKMap = new TreeMap<>();
            this.k = context.getConfiguration().getInt("k", 10);
        }

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            
            String line = key.toString();
            String[] deckField = line.split("_");
            //deckField = GLOBAL_useDeck_062223253f5f6669
            //deckField = WEEK_44_useDeck_062223253f5f6669
            for (DoubleWritable val : values) {
                for (int i = 0; i < nameStats.length; i++) {
                    if (deckField.length == 3) {
                        String[] newDeckField = {deckField[0], deckField[2], deckField[1]};
                        processEntry(newDeckField, val.get(), topKMap, k, deckField[1]);
                    } else {
                        String[] newDeckField = {deckField[0], deckField[1], deckField[3], deckField[2]};
                        processEntry(newDeckField, val.get(), topKMap, k, deckField[2]);
                    }
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (KeyMap type : topKMap.keySet()) {
                for (DeckDescriptor key : topKMap.get(type)) {
                    context.write(new Text(type + "_" + key.getCards()), new DoubleWritable(key.getValue()));
                }
            }
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
