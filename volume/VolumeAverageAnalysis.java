package com.example.stockanalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * this program calculates the average amount of money traded per day for each stock
 * it uses the number of shares traded (volume) and the stock's price at the end of the day
 * the calculation is based on the formula:
 *
 *      average daily dollar volume = total dollar volume / number of days
 *
 * where dollar volume = volume × closing price
 * the result shows which stocks have the highest financial activity on average
 */
public class VolumeAverageAnalysis {

    /**
     * mapper class processes each line of the input csv file and emits:
     * key: stock symbol (e.g., "AAPL")
     * value: the dollar volume for the day (e.g., 5000000.0)
     */
    public static class MapperQ2 extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private boolean isHeader = true; // flag to skip the header row

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // skip the header row (only for the first line of the file)
            if (isHeader) {
                isHeader = false;
                return;
            }

            // split the input line into parts based on commas
            String[] parts = value.toString().split(",");

            // validate that the line has the expected number of columns
            if (parts.length < 8) return;

            // extract the relevant fields: symbol, close price, and volume
            String symbol = parts[1].trim();      // stock symbol
            String closeStr = parts[3].trim();   // closing price
            String volumeStr = parts[7].trim();  // trading volume

            // check if the symbol or closing price is missing
            if (symbol.isEmpty() || closeStr.isEmpty()) {
                return; // skip rows with missing symbol or closing price
            }

            try {
                // parse the close price and volume
                double close = Double.parseDouble(closeStr);
                long volume = Long.parseLong(volumeStr);

                // calculate the dollar volume for the day (volume × close price)
                double dollarVolume = close * volume;

                // emit the stock symbol as the key and the dollar volume as the value
                context.write(new Text(symbol), new DoubleWritable(dollarVolume));
            } catch (NumberFormatException e) {
                // skip invalid or malformed data
            }
        }
    }

    /**
     * reducer class calculates the average daily dollar volume for each stock
     * emits:
     * key: stock symbol
     * value: average daily dollar volume traded
     */
    public static class ReducerQ2 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double totalDollarVolume = 0.0;
            int totalDays = 0;

            // sum up the dollar volumes and count the number of days
            for (DoubleWritable val : values) {
                totalDollarVolume += val.get();
                totalDays += 1; // each value corresponds to one trading day
            }

            // calculate the average daily dollar volume
            double avgDollarVolume = totalDollarVolume / totalDays;

            // emit the stock symbol and the average daily dollar volume
            context.write(key, new DoubleWritable(avgDollarVolume));
        }
    }

    /**
     * main method sets up and configures the hadoop job
     * usage: VolumeAverageAnalysis <input> <output>
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("usage: VolumeAverageAnalysis <input> <output>");
            System.exit(-1);
        }

        // create a hadoop configuration object
        Configuration conf = new Configuration();

        // set up the job configuration
        Job job = Job.getInstance(conf, "VolumeAverageAnalysis");
        job.setJarByClass(VolumeAverageAnalysis.class);
        job.setMapperClass(MapperQ2.class);
        job.setReducerClass(ReducerQ2.class);

        // specify the output key and value types for the mapper and reducer
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // set the input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // run the job and exit with an appropriate status code
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
