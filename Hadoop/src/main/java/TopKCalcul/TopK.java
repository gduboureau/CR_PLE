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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import DataCalcul.DeckStats;

/**
 * This class implements a MapReduce job to find the top K values for different statistics in a dataset.
 */
public class TopK {

    private static String[] nameStats = {"useDeck", "bestClan", "diffForceWin", "winDeck", "nbPlayers"};
    private static final String GLOBAL_PREFIX = "GLOBAL";

    /**
     * Processes an entry by adding it to the map and keeping only the top K values.
     *
     * @param deckField The deck field.
     * @param value The value.
     * @param map The map to store the entries.
     * @param k The number of top values to keep.
     * @param nameStat The name of the statistic.
     */
    private static void processEntry(String[] deckField, Double value, Map<KeyMap, List<DeckDescriptor>> map, int k, String nameStat){
        DeckDescriptor deckValue = null;
        KeyMap keymap = null;
        if (deckField[0].equals(GLOBAL_PREFIX)) {
            deckValue = new DeckDescriptor(deckField[1], value);
            keymap = new KeyMap(deckField[0], nameStat);
        } else {
            deckValue = new DeckDescriptor(deckField[2], value);
            keymap = new KeyMap(deckField[0] + "_" + deckField[1], nameStat);
        }
        
        map.putIfAbsent(keymap, new ArrayList<DeckDescriptor>());
        map.get(keymap).add(deckValue);

        Collections.sort(map.get(keymap)); 

        if (map.get(keymap).size() > k) {
            map.get(keymap).remove(map.get(keymap).size() - 1);
        }
    }
    

    /**
     * This class represents the mapper for the TopK job.
     */
    public static class TopKMapper extends Mapper<Text, DeckStats, Text, DoubleWritable> {

        private Map<KeyMap, List<DeckDescriptor>> topKMap; 
        private int k;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            topKMap = new TreeMap<>();
            this.k = context.getConfiguration().getInt("k", 10);
        }

        /**
         * Maps the input key/value pair to intermediate key/value pairs.
         *
         * @param key The input key.
         * @param value The input value.
         * @param context The context object for the mapper.
         */
        public void map(Text key, DeckStats  value, Context context) throws IOException, InterruptedException {
            try {
                String deck = key.toString();
                String[] deckField = deck.split("_");
    
                processEntry(deckField, value.getBestClan(), topKMap, k, nameStats[1]);
                processEntry(deckField, value.getDiffForceWin(), topKMap, k, nameStats[2]);
                processEntry(deckField, (double) value.getWinDeck(), topKMap, k, nameStats[3]);
                processEntry(deckField, (double) value.getUseDeck(), topKMap, k, nameStats[0]);
                processEntry(deckField, (double) value.getPlayers().size(), topKMap, k, nameStats[4]);
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

    /**
     * This class represents the reducer for the TopK job.
     */
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

            double max = 0;
            for (DoubleWritable value : values) {
                max = Math.max(max, value.get());
            }
            
            String line = key.toString();
            String[] deckField = line.split("_");
            //deckField = GLOBAL_useDeck_062223253f5f6669
            //deckField = WEEK_44_useDeck_062223253f5f6669
            if (deckField[0].equals(GLOBAL_PREFIX)) {
                String[] newDeckField = {deckField[0], deckField[2], deckField[1]};
                processEntry(newDeckField, max, topKMap, k, deckField[1]);
            } else {
                String[] newDeckField = {deckField[0], deckField[1], deckField[3], deckField[2]};
                processEntry(newDeckField, max, topKMap, k, deckField[2]);
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

    /**
     * Runs the TopK job.
     *
     * @param input The input path.
     * @param output The output path.
     * @param k The number of top values to keep.
     * @throws Exception If an error occurs during the job execution.
     */
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
        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
    }
}
