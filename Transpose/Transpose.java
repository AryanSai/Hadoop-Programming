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

public class Transpose
{
  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
  { 
    private final static IntWritable num = new IntWritable(1);
    private Text position = new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
      String[] parts = value.toString().split(";");
      String[] coords=parts[0].toString().split(",");
      String newCoords= coords[1].toString() + "," + coords[0].toString()+";";
      position.set(newCoords);
      int number = Integer.parseInt(parts[1]);
      num.set(number);
      context.write(position, num);
    }
  }

  public static class TransposeReducer extends Reducer<Text,IntWritable,Text,IntWritable>
  {
    private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values,Context context)throws IOException, InterruptedException
    {
      int num = 0;
      if (values.iterator().hasNext()) {
          num = values.iterator().next().get();
      }
      result.set(num);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception
  {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(Transpose.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(TransposeReducer.class);
    job.setReducerClass(TransposeReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
