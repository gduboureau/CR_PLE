import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class Driver {

     public static void main(String[] args) throws Exception {
        // Configurer le premier job (FilterData)
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Filter Data CR");
        job1.setNumReduceTasks(1);
        job1.setJarByClass(FilterData.class);
        job1.setMapperClass(FilterData.FilterMapper.class);
        job1.setReducerClass(FilterData.FilterReducer.class);
        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(Text.class);
        job1.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        System.exit(job1.waitForCompletion(true) ? 0 : 1);
     }
}
