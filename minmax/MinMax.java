package com.example.stockanalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Q1MinMaxClose computes the minimum and maximum closing values of stocks by symbol.
 * Does it over time and for the whole input file.
 */
public class Q1MinMaxClose {

    /**
     * Mapper class that processes each line of the input file.
     * It extracts the stock symbol and closing value, emitting them as key-value pairs.
     */
    public static class MapperQ1 extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private boolean isHeader = true; // Flag to skip the header line

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Skip the header line (first line of the file)
            if (isHeader) {
                isHeader = false;
                return;
            }

            // Split the line into parts based on commas
            String line = value.toString();
            String[] parts = line.split(",");

            // Validate that the line has the expected number of fields
            if (parts.length < 8) return;

            // Extract the stock symbol and closing value
            String symbol = parts[1].trim();
            String closeStr = parts[3].trim();

            // Check if the symbol or closing price is missing
            if (symbol.isEmpty() || closeStr.isEmpty()) {
                return; // Skip rows with missing symbol or closing price
            }

            try {
                // Parse the closing value and write the key-value pair to context
                double closeVal = Double.parseDouble(closeStr);
                context.write(new Text(symbol), new DoubleWritable(closeVal));
            } catch (NumberFormatException e) {
                // Skip invalid numeric data
            }
        }
    }

    /**
     * Reducer class that calculates the minimum and maximum closing values for each stock symbol.
     */
    public static class ReducerQ1 extends Reducer<Text, DoubleWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            // Initialize min and max values
            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;

            // Iterate over all values to find the min and max
            for (DoubleWritable val : values) {
                double d = val.get();
                if (d < min) min = d;
                if (d > max) max = d;
            }

            // Write the results as a single string for each stock symbol
            context.write(key, new Text("MinClose=" + min + ",MaxClose=" + max));
        }
    }

    /**
     * The main method sets up and runs the Hadoop job.
     */
    public static void main(String[] args) throws Exception {
        // Check if input and output paths are provided
        if (args.length < 2) {
            System.err.println("Usage: Q1MinMaxClose <input> <output>");
            System.exit(-1);
        }

        // Create a new Hadoop configuration
        Configuration conf = new Configuration();

        // Set up the job configuration
        Job job = Job.getInstance(conf, "Q1MinMaxClose");
        job.setJarByClass(Q1MinMaxClose.class);

        // Specify the Mapper and Reducer classes
        job.setMapperClass(MapperQ1.class);
        job.setReducerClass(ReducerQ1.class);

        // Specify the output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Set the input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Exit with success or failure code based on job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
