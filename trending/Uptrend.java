package com.example.stockanalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

/**
 * This program calculates the average daily change in closing prices for each stock symbol
 * over the last x days (defaulting to 30 days). It can be used to compare the performance
 * of multiple stocks over the same period.
 */
public class Q4Uptrend {

    /**
     * Mapper class processes each line of the input CSV file and emits:
     * Key: Stock symbol (e.g., "AAPL")
     * Value: A string containing the date and the closing price (e.g., "2023-01-01,145.23")
     */
    public static class MapperQ4 extends Mapper<LongWritable, Text, Text, Text> {
        private boolean isHeader = true; // Flag to skip the header row

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Skip the header row (only for the first line of the file)
            if (isHeader) {
                isHeader = false;
                return;
            }

            // Split the input line into parts based on commas
            String[] parts = value.toString().split(",");

            // Validate that the line has the expected number of columns
            if (parts.length < 8) return;

            // Extract the relevant fields: date, symbol, and close price
            String date = parts[0].trim();        // Date of the record
            String symbol = parts[1].trim();      // Stock symbol
            String closeStr = parts[3].trim();    // Closing price

            // Check if the symbol or closing price is missing
            if (symbol.isEmpty() || closeStr.isEmpty()) {
                return; // Skip rows with missing symbol or closing price
            }

            try {
                // Parse the closing price to ensure it's a valid number
                double closeVal = Double.parseDouble(closeStr);

                // Emit the stock symbol as the key and "date,close" as the value
                context.write(new Text(symbol), new Text(date + "," + closeVal));
            } catch (NumberFormatException e) {
                // Skip invalid or malformed closing price data
            }
        }
    }

    /**
     * Reducer class aggregates the closing price data for each stock symbol,
     * calculates the average daily change in closing price over the last x days,
     * and emits:
     * Key: Stock symbol
     * Value: Average daily change in closing price
     */
    public static class ReducerQ4 extends Reducer<Text, Text, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int x = conf.getInt("x", 30); // Number of days to consider, default is 30

            // Store records as [date, close] pairs in a list
            ArrayList<String[]> records = new ArrayList<>();
            for (Text val : values) {
                String[] parts = val.toString().split(",");
                if (parts.length == 2) {
                    records.add(parts);
                }
            }

            // Sort the records by date (assumes the date is in a parsable format)
            Collections.sort(records, Comparator.comparing(o -> o[0]));

            // Extract the last x days of data
            int start = Math.max(0, records.size() - x);
            List<String[]> recent = records.subList(start, records.size());

            // If there are fewer than 2 data points, skip this stock (not enough data to calculate changes)
            if (recent.size() < 2) {
                return;
            }

            // Calculate the average daily change in closing price
            double sumChange = 0.0;
            int count = 0;
            double prevClose = Double.parseDouble(recent.get(0)[1]); // Initialize with the first day's closing price
            for (int i = 1; i < recent.size(); i++) {
                double currClose = Double.parseDouble(recent.get(i)[1]); // Current day's closing price
                double change = currClose - prevClose; // Daily change in closing price
                sumChange += change;
                count++;
                prevClose = currClose; // Update previous day's closing price
            }

            // Calculate the average daily change
            double avgChange = sumChange / count;

            // Emit the stock symbol and the average daily change
            context.write(key, new DoubleWritable(avgChange));
        }
    }

    /**
     * The main method sets up and configures the Hadoop job.
     * Usage: Q4Uptrend [-Dx=<days>] <input> <output>
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: Q4Uptrend [-Dx=<days>] <input> <output>");
            System.exit(-1);
        }

        // Create a Hadoop configuration object
        Configuration conf = new Configuration();

        // Set up the job configuration
        Job job = Job.getInstance(conf, "Q4Uptrend");
        job.setJarByClass(Q4Uptrend.class);
        job.setMapperClass(MapperQ4.class);
        job.setReducerClass(ReducerQ4.class);

        // Specify the output key and value types for the job
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set the input and output paths
        FileInputFormat.addInputPath(job, new Path(args[args.length - 2]));
        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));

        // Run the job and exit with an appropriate status code
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
