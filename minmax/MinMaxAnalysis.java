package com.example.stockanalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * minmaxanalysis computes the min and max closing values of stocks by symbol
 * also tracks the starting date and calculates the percentage increase
 */
public class MinMaxAnalysis {

    /**
     * mapper class processes each line of the input file
     * emits the stock symbol and a composite value (date and closing value)
     */
    public static class MapperQ1 extends Mapper<LongWritable, Text, Text, Text> {
        private boolean isHeader = true;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // skip the header line (first line of the file)
            if (isHeader) {
                isHeader = false;
                return;
            }

            // split the line into parts based on commas
            String line = value.toString();
            String[] parts = line.split(",");

            if (parts.length < 8) return; // validate number of fields

            String symbol = parts[1].trim();
            String date = parts[0].trim();
            String closeStr = parts[3].trim();

            if (symbol.isEmpty() || date.isEmpty() || closeStr.isEmpty()) return;

            try {
                // emit symbol as key and a composite string of date and closing value as value
                double closeVal = Double.parseDouble(closeStr);
                context.write(new Text(symbol), new Text(date + "," + closeVal));
            } catch (NumberFormatException e) {
                // skip invalid numeric data
            }
        }
    }

    /**
     * reducer class calculates the min, max closing values,
     * starting date, and percentage increase for each stock symbol
     */
    public static class ReducerQ1 extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;

            String startDate = null;
            double startValue = 0.0;

            PriorityQueue<String[]> dateQueue = new PriorityQueue<>(Comparator.comparing(o -> o[0]));

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                if (parts.length != 2) continue;

                String date = parts[0];
                double closeVal;

                try {
                    closeVal = Double.parseDouble(parts[1]);
                } catch (NumberFormatException e) {
                    continue;
                }

                // update min and max
                if (closeVal < min) min = closeVal;
                if (closeVal > max) max = closeVal;

                // track earliest date and its closing value
                dateQueue.offer(new String[]{date, String.valueOf(closeVal)});
            }

            // determine starting date and value
            if (!dateQueue.isEmpty()) {
                String[] startEntry = dateQueue.poll();
                startDate = startEntry[0];
                startValue = Double.parseDouble(startEntry[1]);
            }

            // calculate percentage increase
            double percentIncrease = (startValue != 0) ? ((max - startValue) / startValue) * 100 : 0;

            // output the result
            context.write(key, new Text("startdate=" + startDate +
                    ", startvalue=" + startValue +
                    ", minclose=" + min +
                    ", maxclose=" + max +
                    ", percentincrease=" + percentIncrease + "%"));
        }
    }

    /**
     * the main method sets up and runs the hadoop job
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("usage: minmaxanalysis <input> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "minmaxanalysis");
        job.setJarByClass(MinMaxAnalysis.class);

        job.setMapperClass(MapperQ1.class);
        job.setReducerClass(ReducerQ1.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}