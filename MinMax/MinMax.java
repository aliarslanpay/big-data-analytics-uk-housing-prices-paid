import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;

public class MinMax {
    public static final int PRICE = 1;
    public static final int DATE_OF_TRANSFER = 2;

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.seperator", ",");  //csv format

        if (args.length != 2){
            System.out.println("Usage:<in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "MinMax");
        job.setJarByClass(MinMax.class);
        job.setMapperClass(MinMaxMapper.class);
        job.setReducerClass(MinMaxReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        Files.deleteIfExists(new File(args[1]).toPath());
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class MinMaxMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        final Calendar cal = Calendar.getInstance();
        final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        DoubleWritable price = new DoubleWritable();
        String year;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            long val = ((LongWritable) key).get();
            if (val == 0)
                return;
            String[] parts = value.toString().split("[,]");
            try{
                Date date = formatter.parse(parts[DATE_OF_TRANSFER]);
                cal.setTime(date);
                year = String.valueOf(cal.get(Calendar.YEAR));
                price.set(Double.parseDouble(parts[PRICE]));
            } catch (ParseException e) {
                e.printStackTrace();
            }
            context.write(new Text(year), price);
        }
    }

    public static class MinMaxReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text year, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
            double min = 9999999999999999d;
            double max = 0;
            double temp;
            for (DoubleWritable val : values) {
                temp = val.get();
                if(temp > max)
                    max = temp;
                else if (temp < min)
                    min = temp;
            }
            context.write(year, new DoubleWritable(min));
            context.write(year, new DoubleWritable(max));
        }
    }
}


