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
 * this program calculates the average daily change in closing prices for each stock symbol
 * over the last x days (defaulting to 30 days). can be used to compare performance
 * of multiple stocks over the same period
 */
public class TrendingAnalysis {

    /**
     * mapper class processes each line of the input csv file and emits:
     * key: stock symbol (e.g., "AAPL")
     * value: a string containing the date and the closing price (e.g., "2023-01-01,145.23")
     */
    public static class MapperQ4 extends Mapper<LongWritable, Text, Text, Text> {
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

            // extract the relevant fields: date, symbol, and close price
            String date = parts[0].trim()        // date of the record
            String symbol = parts[1].trim()      // stock symbol
            String closeStr = parts[3].trim()    // closing price

            // check if the symbol or closing price is missing
            if (symbol.isEmpty() || closeStr.isEmpty()) {
                return // skip rows with missing symbol or closing price
            }

            try {
                // parse the closing price to ensure it's a valid number
                double closeVal = Double.parseDouble(closeStr)

                // emit the stock symbol as the key and "date,close" as the value
                context.write(new Text(symbol), new Text(date + "," + closeVal))
            } catch (NumberFormatException e) {
                // skip invalid or malformed closing price data
            }
        }
    }

    /**
     * reducer class aggregates the closing price data for each stock symbol,
     * calculates the average daily change in closing price over the last x days,
     * and emits:
     * key: stock symbol
     * value: average daily change in closing price
     */
    public static class ReducerQ4 extends Reducer<Text, Text, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration()
            int x = conf.getInt("x", 30) // number of days to consider, default is 30

            // store records as [date, close] pairs in a list
            ArrayList<String[]> records = new ArrayList<>()
            for (Text val : values) {
                String[] parts = val.toString().split(",")
                if (parts.length == 2) {
                    records.add(parts)
                }
            }

            // sort the records by date (assumes the date is in a parsable format)
            Collections.sort(records, Comparator.comparing(o -> o[0]))

            // extract the last x days of data
            int start = Math.max(0, records.size() - x)
            List<String[]> recent = records.subList(start, records.size())

            // if there are fewer than 2 data points, skip this stock (not enough data to calculate changes)
            if (recent.size() < 2) {
                return
            }

            // calculate the average daily change in closing price
            double sumChange = 0.0
            int count = 0
            double prevClose = Double.parseDouble(recent.get(0)[1]) // initialize with the first day's closing price
            for (int i = 1; i < recent.size(); i++) {
                double currClose = Double.parseDouble(recent.get(i)[1]) // current day's closing price
                double change = currClose - prevClose // daily change in closing price
                sumChange += change
                count++
                prevClose = currClose // update previous day's closing price
            }

            // calculate the average daily change
            double avgChange = sumChange / count

            // emit the stock symbol and the average daily change
            context.write(key, new DoubleWritable(avgChange))
        }
    }

    /**
     * the main method sets up and configures the hadoop job
     * usage: Q4Uptrend [-Dx=<days>] <input> <output>
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("usage: trendinganalysis [-Dx=<days>] <input> <output>")
            System.exit(-1)
        }

        // create a hadoop configuration object
        Configuration conf = new Configuration()

        // set up the job configuration
        Job job = Job.getInstance(conf, "trendinganalysis")
        job.setJarByClass(TrendingAnalysis.class)
        job.setMapperClass(MapperQ4.class)
        job.setReducerClass(ReducerQ4.class)

        // specify the output key and value types for the job
        job.setOutputKeyClass(Text.class)
        job.setOutputValueClass(Text.class)

        // set the input and output paths
        FileInputFormat.addInputPath(job, new Path(args[args.length - 2]))
        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]))

        // run the job and exit with an appropriate status code
        System.exit(job.waitForCompletion(true) ? 0 : 1)
    }
}
