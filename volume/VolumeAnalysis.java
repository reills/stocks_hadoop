package com.example.stockanalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;



/**
 * This program calculates the average amount of money traded per day for each stock.
 * It considers the number of shares traded (volume) and the stock's price at the end of the day.
 * The calculation is based on the formula:
 * 
 *      Average Daily Dollar Volume = Total Dollar Volume / Number of Days
 * 
 * where Dollar Volume = Volume × Closing Price.
 * The result shows which stocks consistently have the highest financial activity on average.
 */

public class AverageDollarVolume {

    /**
     * Mapper class processes each line of the input CSV file and emits:
     * Key: Stock symbol (e.g., "AAPL").
     * Value: A pair containing the dollar volume for the day and the count (e.g., "5000000,1").
     */
    public static class MapperQ2 extends Mapper<LongWritable, Text, Text, Text> {
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

            // Extract the relevant fields: symbol, close price, and volume
            String symbol = parts[1].trim();      // Stock symbol
            String closeStr = parts[3].trim();   // Closing price
            String volumeStr = parts[7].trim();  // Trading volume

            // Check if the symbol or closing price is missing
            if (symbol.isEmpty() || closeStr.isEmpty()) {
                return; // Skip rows with missing symbol or closing price
            }

            try {
                // Parse the close price and volume
                double close = Double.parseDouble(closeStr);
                long volume = Long.parseLong(volumeStr);

                // Calculate the dollar volume for the day (volume × close price)
                double dollarVolume = close * volume;

                // Emit the stock symbol as the key and "dollarVolume,1" as the value
                context.write(new Text(symbol), new Text(dollarVolume + ",1"));
            } catch (NumberFormatException e) {
                // Skip invalid or malformed data
            }
        }
    }

    /**
     * Reducer class calculates the average daily dollar volume for each stock.
     * Emits:
     * Key: Stock symbol.
     * Value: Average daily dollar volume traded.
     */
    public static class ReducerQ2 extends Reducer<Text, Text, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double totalDollarVolume = 0.0;
            int totalDays = 0;

            // Sum up the dollar volumes and count the number of days
            for (Text val : values) {
                String[] parts = val.toString().split(",");
                if (parts.length == 2) {
                    double dollarVolume = Double.parseDouble(parts[0]);
                    int dayCount = Integer.parseInt(parts[1]);

                    totalDollarVolume += dollarVolume;
                    totalDays += dayCount;
                }
            }

            // Calculate the average daily dollar volume
            double avgDollarVolume = totalDollarVolume / totalDays;

            // Emit the stock symbol and the average daily dollar volume
            context.write(key, new DoubleWritable(avgDollarVolume));
        }
    }

    /**
     * Main method sets up and configures the Hadoop job.
     * Usage: AverageDollarVolume <input> <output>
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: AverageDollarVolume <input> <output>");
            System.exit(-1);
        }

        // Create a Hadoop configuration object
        Configuration conf = new Configuration();

        // Set up the job configuration
        Job job = Job.getInstance(conf, "AverageDollarVolume");
        job.setJarByClass(AverageDollarVolume.class);
        job.setMapperClass(MapperQ2.class);
        job.setReducerClass(ReducerQ2.class);

        // Specify the output key and value types for the job
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Set the input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Run the job and exit with an appropriate status code
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
