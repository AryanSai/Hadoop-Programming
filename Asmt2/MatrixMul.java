import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMul {

    public static class MatrixMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            String matrix = tokens[0];
            int row = Integer.parseInt(tokens[1]);
            int col = Integer.parseInt(tokens[2]);
            int val = Integer.parseInt(tokens[3]);

            if (matrix.equals("A")) {
                for (int k = 0; k < context.getConfiguration().getInt("p", 1); k++) {
                    context.write(new Text(row + "," + k), new Text(matrix + "," + col + "," + val));
                }
            } else if (matrix.equals("B")) {
                for (int i = 0; i < context.getConfiguration().getInt("m", 1); i++) {
                    context.write(new Text(i + "," + col), new Text(matrix + "," + row + "," + val));
                }
            }
        }
    }

    public static class MatrixReducer extends Reducer<Text, Text, Text, IntWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int[] rowA = new int[context.getConfiguration().getInt("n", 1)];
            int[] colB = new int[context.getConfiguration().getInt("n", 1)];

            for (Text val : values) {
                String[] tokens = val.toString().split(",");
                String matrix = tokens[0];
                int index = Integer.parseInt(tokens[1]);
                int value = Integer.parseInt(tokens[2]);

                if (matrix.equals("A")) {
                    rowA[index] = value;
                } else if (matrix.equals("B")) {
                    colB[index] = value;
                }
            }

            int result = 0;
            for (int i = 0; i < context.getConfiguration().getInt("n", 1); i++) {
                result += rowA[i] * colB[i];
            }
            context.write(key, new IntWritable(result));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("m", Integer.parseInt(args[2]));
        conf.setInt("n", Integer.parseInt(args[3]));
        conf.setInt("p", Integer.parseInt(args[4]));
        Job job = Job.getInstance(conf, "matrix multiplication");
        job.setJarByClass(MatrixMul.class);
        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
