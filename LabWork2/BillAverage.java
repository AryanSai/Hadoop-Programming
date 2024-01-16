import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BillAverage
{

  public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, IntWritable>
  {

    private final static IntWritable bill_id = new IntWritable(1);
    private final static IntWritable bill_amount1 = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
     String [] result = value.toString().split(",");
     int branch_id = Integer.parseInt(result[0]);
     int bill_amount = Integer.parseInt(result[1]);
     bill_id.set(branch_id);
     bill_amount1.set(bill_amount);
     context.write(bill_id, bill_amount1);
    }
  }

  public static class IntSumReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>
  {
    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values,Context context)throws IOException, InterruptedException
    {
      int sum = 0;
      int count = 0;
      for (IntWritable val : values)
      {
        sum += val.get();
        count++;
      }
      result.set(sum / count);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception
  {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(BillAverage.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
