import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Select
{

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>
  {
    private Text nameText = new Text();
    private Text gradeText = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
      String [] rows = value.toString().split(",");
      String grade = rows[4];
      String name = rows[1];
      gradeText.set(grade);
      nameText.set(name);
      context.write(gradeText, nameText);
    }
  }

  public static class SelectReducer extends Reducer<Text,Text,Text,Text>
  {

    public void reduce(Text key, Iterable<Text> values,Context context)throws IOException, InterruptedException
    { 
      for (Text val : values)
      {
        if(!key.toString().equals("O")){
          context.write(key, val);
        }
      }
    }
  }

  public static void main(String[] args) throws Exception
  {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(Select.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(SelectReducer.class);
    job.setReducerClass(SelectReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}