import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;

public class Median {
    public static final int PRICE = 1;
    public static final int DATE_OF_TRANSFER = 2;

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.seperator", ",");

        if (args.length != 2){
            System.out.println("Usage:<in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Median");
        job.setJarByClass(Median.class);
        job.setMapperClass(MedianMapper.class);
        job.setReducerClass(MedianReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        Files.deleteIfExists(new File(args[1]).toPath());
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class MedianMapper extends Mapper<Object, Text, Text, DoubleWritable> {
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

    public static class MedianReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text year, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
            List<Double> list = new ArrayList<>();
            int length;
            double median;
            double medianSum;
            
            for (DoubleWritable val : values)
                list.add(val.get());
            
            Collections.sort(list);
            length = list.size();

            if(length == 2){
                medianSum = list.get(0) + list.get(1);
                median = medianSum / 2;
            }
            else if(length % 2 == 0){
                medianSum = list.get((length/2) - 1) + list.get(length/2);
                median = medianSum/2;
            }
            else {
                median = list.get(length/2);
            }
            
            context.write(year, new DoubleWritable(median));
        }
    }
}


