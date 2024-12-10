package com.example.stockanalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * this program calculates the top stock mover (highest percentage gain) for each trading day
 * based on opening and closing price
 */
public class BiggestMoverAnalysis {

    /**
     * mapper class processes each line of the input csv file and emits:
     * key: date (e.g., "2023-01-01")
     * value: a string containing the stock symbol, closing price, and opening price
     *        (e.g., "AAPL,145.0,140.0").
     */
    public static class MapperQ3 extends Mapper<LongWritable, Text, Text, Text> {
        private boolean isHeader = true; // flag to skip the header row

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // skip the header row (only for the first line of the file)
            if (isHeader) {
                isHeader = false
                return
            }

            // split the input line into parts based on commas
            String[] parts = value.toString().split(",")

            // validate that the line has the expected number of columns
            if (parts.length < 8) return

            // extract the relevant fields: date, symbol, closing price, and opening price
            String date = parts[0].trim()       // trading date
            String symbol = parts[1].trim()     // stock symbol
            String closeStr = parts[3].trim()   // closing price
            String openStr = parts[6].trim()    // opening price
            try {
                // parse the closing and opening prices to ensure they are valid numbers
                double closeVal = Double.parseDouble(closeStr)
                double openVal = Double.parseDouble(openStr)

                // emit the date as the key and "symbol,close,open" as the value
                context.write(new Text(date), new Text(symbol + "," + closeVal + "," + openVal))
            } catch (NumberFormatException e) {
                // skip invalid or malformed data
            }
        }
    }

    /**
     * reducer class processes the stock data for each trading day and identifies:
     * - the stock with the highest percentage change in price for that day
     * emits:
     * key: date
     * value: stock symbol with the highest percentage change, the change percentage,
     *        and the corresponding opening and closing prices
     */
    public static class ReducerQ3 extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String topSymbol = null // stock symbol with the highest percentage change
            double topChange = Double.NEGATIVE_INFINITY // initialize to a very low value
            double topOpen = 0.0 // track the opening price for the top mover
            double topClose = 0.0 // track the closing price for the top mover

            // iterate through all stock data for the given date
            for (Text val : values) {
                // parse the value into parts: symbol, close price, open price
                String[] parts = val.toString().split(",")
                if (parts.length == 3) {
                    String symbol = parts[0]       // stock symbol
                    double closeVal = Double.parseDouble(parts[1]) // closing price
                    double openVal = Double.parseDouble(parts[2])  // opening price

                    // ensure the open price is non-zero to avoid division by zero
                    if (openVal != 0) {
                        // calculate the percentage change
                        double changePerc = ((closeVal - openVal) / openVal) * 100.0

                        // update the top mover if this stock's percentage change is higher
                        if (changePerc > topChange) {
                            topChange = changePerc
                            topSymbol = symbol
                            topOpen = openVal
                            topClose = closeVal
                        }
                    }
                }
            }

            // emit the date and the stock with the highest percentage change,
            // including the opening and closing prices
            if (topSymbol != null) {
                String result = String.format("%s,%s,%f%%,Open: %f,Close: %f",
                        topSymbol, topChange, topChange, topOpen, topClose)
                context.write(key, new Text(result))
            }
        }
    }

    /**
     * main method sets up and configures the hadoop job to calculate daily top stock movers
     * usage: Q3DailyTopMovers <input> <output>
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("usage: biggestmoveranalysis <input> <output>")
            System.exit(-1)
        }

        // create a hadoop configuration object
        Configuration conf = new Configuration()

        // set up the job configuration
        Job job = Job.getInstance(conf, "biggestmoveranalysis")
        job.setJarByClass(BiggestMoverAnalysis.class)
        job.setMapperClass(MapperQ3.class)
        job.setReducerClass(ReducerQ3.class)

        // specify the output key and value types for the job
        job.setOutputKeyClass(Text.class)
        job.setOutputValueClass(Text.class)

        // set the input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]))
        FileOutputFormat.setOutputPath(job, new Path(args[1]))

        // run the job and exit with an appropriate status code
        System.exit(job.waitForCompletion(true) ? 0 : 1)
    }
}
