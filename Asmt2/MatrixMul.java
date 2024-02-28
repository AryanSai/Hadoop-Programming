import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MatrixMul {

    public static class Map extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");

            String matrixName = tokens[0]; 
            int row = Integer.parseInt(tokens[1]);
            int col = Integer.parseInt(tokens[2]);
            int val = Integer.parseInt(tokens[3]);

            if (matrixName.equals("A")) {
                // Emit intermediate values with key as (col, row) for each element of A
                for (int k = 1; k <= 100; k++) {
                    context.write(new Text(col + "," + k), new Text(matrixName + "," + row + "," + val));
                }
            } else { // matrix B
                // Emit intermediate values with key as (row, col) for each element of B
                for (int i = 1; i <= 100; i++) {
                    context.write(new Text(i + "," + row), new Text(matrixName + "," + col + "," + val));
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String> aVals = new ArrayList<String>();
            List<String> bVals = new ArrayList<String>();

            for (Text val : values) {
                String[] tokens = val.toString().split(",");
                if (tokens[0].equals("A")) {
                    aVals.add(val.toString());
                } else {
                    bVals.add(val.toString());
                }
            }

            for (String aVal : aVals) {
                String[] aToken = aVal.split(",");
                int row = Integer.parseInt(aToken[1]);
                int aValue = Integer.parseInt(aToken[2]);
                for (String bVal : bVals) {
                    String[] bToken = bVal.split(",");
                    int col = Integer.parseInt(bToken[1]);
                    int bValue = Integer.parseInt(bToken[2]);

                    int result = aValue * bValue;
                    context.write(null, new Text(row + "," + col + "," + result));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Matrix Multiplication");

        job.setJarByClass(MatrixMul.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
