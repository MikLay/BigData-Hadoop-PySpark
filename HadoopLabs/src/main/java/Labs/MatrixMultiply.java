package Labs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class MatrixMultiply {
    public static final String rowAConf = "rowA";
    public static final String columnBConf = "columnB";
    public static final String columnAConf = "columnA";
    public static int rowA = 0;
    public static int columnB = 0;
    public static int columnA = 0;
    public static String positionValueSeparator = " ";
    public static String rowColumnSeparator = ",";

    public static final String valueSeparator = ",";
    public static final String argsDimensionSeparator = "-";
    public static final String firstMatrix = "A";
    public static final String secondMatrix = "B";

    public static class PositionMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text mapKey = new Text();
        private Text mapValue = new Text();

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            Configuration configuration = new Configuration();
            columnB = Integer.parseInt(configuration.get(columnBConf));
            rowA = Integer.parseInt(configuration.get(rowAConf));
        }


        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {

            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            String matrix = fileName.contains(firstMatrix) ? firstMatrix : secondMatrix;

            String[] positionAndValue = value.toString().split(positionValueSeparator);
            int val = Integer.parseInt(positionAndValue[1]);
            String[] rowAndColumn = positionAndValue[0].split(rowColumnSeparator);
            int row = Integer.parseInt(rowAndColumn[0]);
            int column = Integer.parseInt(rowAndColumn[1]);


            if (matrix.equals(firstMatrix)) {
                for (int k = 0; k < columnB; k++) {
                    mapKey.set(row + rowColumnSeparator + k);
                    mapValue.set(firstMatrix + valueSeparator + column + valueSeparator + val);
                    context.write(mapKey, mapValue);
                }
            } else {
                for (int i = 0; i < rowA; i++) {
                    mapKey.set(i + rowColumnSeparator + column);
                    mapValue.set(secondMatrix + valueSeparator + row + valueSeparator + val);
                    context.write(mapKey, mapValue);
                }
            }
        }
    }


    public static class MatrixReducer extends Reducer<Text, Text, Text, Text> {

        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            columnA = Integer.parseInt(conf.get(columnAConf));
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            int[] A = new int[columnA];
            int[] B = new int[columnA];

            for (Text val : values) {
                String[] MatrixIndexValue = val.toString().split(valueSeparator);
                if (MatrixIndexValue[0].equals(firstMatrix)) {
                    A[Integer.parseInt(MatrixIndexValue[1])] = Integer.parseInt(MatrixIndexValue[2]);
                } else
                    B[Integer.parseInt(MatrixIndexValue[1])] = Integer.parseInt(MatrixIndexValue[2]);
            }

            for (int j = 0; j < columnA; j++) {
                sum += A[j] * B[j];
            }
            context.write(key, new Text(Integer.toString(sum)));
            sum = 0;
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 4) {
            System.err.println("Usage: MatrixMultiply <in File> <matrix A dimension> <in File> <matrix B dimension> <out>");
            System.exit(2);
        }
        // Set dimensions from args
        String[] infoTupleA = args[1].split(argsDimensionSeparator);
        rowA = Integer.parseInt(infoTupleA[1]);
        columnA = Integer.parseInt(infoTupleA[2]);
        String[] infoTupleB = args[3].split(argsDimensionSeparator);
        columnB = Integer.parseInt(infoTupleB[2]);

        conf.setInt(rowAConf, rowA);
        conf.setInt(columnAConf, columnA);
        conf.setInt(columnBConf, columnB);

        Job job = Job.getInstance(conf, "Matrix Multiplication");
        job.setJarByClass(MatrixMultiply.class);
        job.setMapperClass(PositionMapper.class);
        job.setReducerClass(MatrixReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // First and 3rd args are input files
        FileInputFormat.setInputPaths(job, new Path(args[0]), new Path(args[2]));
        // Last arg is output folder
        FileOutputFormat.setOutputPath(job, new Path(args[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}