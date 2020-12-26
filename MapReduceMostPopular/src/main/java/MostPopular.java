import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class MostPopular {

    public static class ItemFollowedMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

        private IntWritable keyIntWritable = new IntWritable();
        private IntWritable valueIntWritable = new IntWritable(1);
        private Configuration conf = new Configuration();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line, ",");
            int item = 0;
            for (int i = 0; i < 7; i++) {
                String token = tokenizer.nextToken();
                if (token.equals("user_id")) return;
                if (i == 1) item = Integer.parseInt(token);
                if (i == 5 && !token.equals("1111")) return;
                if (i == 6 && token.equals("0")) return;
            }
            keyIntWritable.set(item);
            context.write(keyIntWritable, valueIntWritable);
        }

    }

    public static class MerchantFollowedMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

        private IntWritable keyIntWritable = new IntWritable();
        private IntWritable valueIntWritable = new IntWritable(1);
        private Configuration conf = new Configuration();
        private Set<Integer> ageU30Set = new TreeSet<Integer>();

        public void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            BufferedReader fis = new BufferedReader(new FileReader("user_info_format1.csv"));
            String line = null;
            while ((line = fis.readLine()) != null) {
                StringTokenizer tokenizer = new StringTokenizer(line, ",");
                int user = 0;
                boolean ok = true;
                for (int i = 0; i < 2; i++) {
                    if (!tokenizer.hasMoreElements()){
                        ok = false;
                        break;
                    }
                    String token = tokenizer.nextToken();
                    if (token.equals("user_id")) {
                        ok = false;
                        break;
                    }
                    if (i == 0) user = Integer.parseInt(token);
                    if (i == 1 && !(token.equals("1") || token.equals("2") || token.equals("3"))) {
                        ok = false;
                        break;
                    }
                }
                if (ok) ageU30Set.add(user);
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line, ",");
            int user = 0;
            int merchant = 0;
            for (int i = 0; i < 7; i++) {
                String token = tokenizer.nextToken();
                if (token.equals("user_id")) return;
                if (i == 1) user = Integer.parseInt(token);
                else if (i == 3) merchant = Integer.parseInt(token);
                else if (i == 5 && !token.equals("1111")) return;
                else if (i == 6 && token.equals("0")) return;
            }
            if (!ageU30Set.contains(user)) return;
            keyIntWritable.set(merchant);
            context.write(keyIntWritable, valueIntWritable);
        }

    }

    public static class IntSumReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private IntWritable valueIntWritable = new IntWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            valueIntWritable.set(sum);
            context.write(key, valueIntWritable);
        }
    }

    public static class IntWritableDecreaseComparator extends IntWritable.Comparator {
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static class OrderRecordWriter extends RecordWriter<IntWritable, IntWritable> {

        FSDataOutputStream fos = null;
        Integer order = 0;

        public OrderRecordWriter(TaskAttemptContext job) {
            FileSystem fs;
            try {
                fs = FileSystem.get(job.getConfiguration());
                String outputDir = job.getConfiguration().get("mapred.output.dir");
                Path outputPath = new Path(outputDir + "/out.txt");
                fos = fs.create(outputPath);
            } catch (IOException e) {
                System.err.println("Caught exception while getting the configuration " + StringUtils.stringifyException(e));
            }
        }

        public void write(IntWritable key, IntWritable value) throws IOException, InterruptedException {
            if (order > 99) return;
            fos.write(((++order).toString() + ": " + value.toString() + ", " + key.toString() + "\n").getBytes());
        }

        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            IOUtils.closeStream(fos);
        }
    }

    public static class OrderOutputFormat extends FileOutputFormat<IntWritable, IntWritable> {
        @Override
        public RecordWriter<IntWritable, IntWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
            return new OrderRecordWriter(job);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();

        Job job = Job.getInstance(conf, "Most Popular Item");
        job.setJarByClass(MostPopular.class);
        job.setMapperClass(ItemFollowedMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("temp/temp_0"));

        if (!job.waitForCompletion(true)) System.exit(1);

        Job sortJob = Job.getInstance(conf, "Value Sort");
        sortJob.setJarByClass(MostPopular.class);
        sortJob.setInputFormatClass(SequenceFileInputFormat.class);
        sortJob.setMapperClass(InverseMapper.class);
        sortJob.setNumReduceTasks(1);
        sortJob.setOutputKeyClass(IntWritable.class);
        sortJob.setOutputValueClass(IntWritable.class);
        sortJob.setSortComparatorClass(IntWritableDecreaseComparator.class);
        sortJob.setOutputFormatClass(OrderOutputFormat.class);

        FileInputFormat.addInputPath(sortJob, new Path("temp/temp_0"));
        FileOutputFormat.setOutputPath(sortJob, new Path("temp/output_0"));

        if (!sortJob.waitForCompletion(true)) System.exit(1);

        Job job1 = Job.getInstance(conf, "Most Popular Merchant");
        job1.setJarByClass(MostPopular.class);
        job1.setMapperClass(MerchantFollowedMapper.class);
        job1.setCombinerClass(IntSumReducer.class);
        job1.setReducerClass(IntSumReducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        job1.addCacheFile(new Path("user_info_format1.csv").toUri());
        FileInputFormat.addInputPath(job1, new Path("input"));
        FileOutputFormat.setOutputPath(job1, new Path("temp/temp_1"));

        if (!job1.waitForCompletion(true)) System.exit(1);

        Job sortJob1 = Job.getInstance(conf, "Value Sort");
        sortJob1.setJarByClass(MostPopular.class);
        sortJob1.setInputFormatClass(SequenceFileInputFormat.class);
        sortJob1.setMapperClass(InverseMapper.class);
        sortJob1.setNumReduceTasks(1);
        sortJob1.setOutputKeyClass(IntWritable.class);
        sortJob1.setOutputValueClass(IntWritable.class);
        sortJob1.setSortComparatorClass(IntWritableDecreaseComparator.class);
        sortJob1.setOutputFormatClass(OrderOutputFormat.class);

        FileInputFormat.addInputPath(sortJob1, new Path("temp/temp_1"));
        FileOutputFormat.setOutputPath(sortJob1, new Path("temp/output_1"));

        if (!sortJob1.waitForCompletion(true)) System.exit(1);

        System.exit(0);

    }

}
