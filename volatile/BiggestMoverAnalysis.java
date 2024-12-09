package com.example.stockanalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * This program calculates the top stock mover (highest percentage gain) for each trading day
 * based on opening and closing price
 */
public class BiggestMoverAnalysis {

    /**
     * Mapper class processes each line of the input CSV file and emits:
     * Key: Date (e.g., "2023-01-01")
     * Value: A string containing the stock symbol, closing price, and opening price
     *        (e.g., "AAPL,145.0,140.0").
     */
    public static class MapperQ3 extends Mapper<LongWritable, Text, Text, Text> {
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

            // Extract the relevant fields: date, symbol, closing price, and opening price
            String date = parts[0].trim();       // Trading date
            String symbol = parts[1].trim();     // Stock symbol
            String closeStr = parts[3].trim();   // Closing price
            String openStr = parts[6].trim();    // Opening price

            // Check if the symbol or closing price is missing
            if (symbol.isEmpty() || closeStr.isEmpty()) {
                return; // Skip rows with missing symbol or closing price
            }

            try {
                // Parse the closing and opening prices to ensure they are valid numbers
                double closeVal = Double.parseDouble(closeStr);
                double openVal = Double.parseDouble(openStr);

                // Emit the date as the key and "symbol,close,open" as the value
                context.write(new Text(date), new Text(symbol + "," + closeVal + "," + openVal));
            } catch (NumberFormatException e) {
                // Skip invalid or malformed data
            }
        }
    }

    /**
     * Reducer class processes the stock data for each trading day and identifies:
     * - The stock with the highest percentage change in price for that day.
     * Emits:
     * Key: Date
     * Value: Stock symbol with the highest percentage change and the change percentage
     *        (e.g., "AAPL,3.57%").
     */
    public static class ReducerQ3 extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String topSymbol = null; // Stock symbol with the highest percentage change
            double topChange = Double.NEGATIVE_INFINITY; // Initialize to a very low value

            // Iterate through all stock data for the given date
            for (Text val : values) {
                // Parse the value into parts: symbol, close price, open price
                String[] parts = val.toString().split(",");
                if (parts.length == 3) {
                    String symbol = parts[0];       // Stock symbol
                    double closeVal = Double.parseDouble(parts[1]); // Closing price
                    double openVal = Double.parseDouble(parts[2]);  // Opening price

                    // Ensure the open price is non-zero to avoid division by zero
                    if (openVal != 0) {
                        // Calculate the percentage change
                        double changePerc = ((closeVal - openVal) / openVal) * 100.0;

                        // Update the top mover if this stock's percentage change is higher
                        if (changePerc > topChange) {
                            topChange = changePerc;
                            topSymbol = symbol;
                        }
                    }
                }
            }

            // Emit the date and the stock with the highest percentage change
            if (topSymbol != null) {
                context.write(key, new Text(topSymbol + "," + topChange + "%"));
            }
        }
    }

    /**
     * Main method sets up and configures the Hadoop job to calculate daily top stock movers.
     * Usage: Q3DailyTopMovers <input> <output>
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: BiggestMoverAnalysis <input> <output>");
            System.exit(-1);
        }

        // Create a Hadoop configuration object
        Configuration conf = new Configuration();

        // Set up the job configuration
        Job job = Job.getInstance(conf, "BiggestMoverAnalysis");
        job.setJarByClass(BiggestMoverAnalysis.class);
        job.setMapperClass(MapperQ3.class);
        job.setReducerClass(ReducerQ3.class);

        // Specify the output key and value types for the job
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set the input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Run the job and exit with an appropriate status code
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
