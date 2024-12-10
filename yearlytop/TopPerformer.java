package com.example.stockanalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * this program computes how each stock performed over the last 'x' days ("the last year")
 * and also gives the start date of that period for each stock
 *
 * by default:
 * hadoop jar "$JAR_NAME" com.example.stockanalysis.TopPerformer -Dx=365 "$HDFS_INPUT_DIR" "$HDFS_OUTPUT_DIR/trending"
 * 
 * 'x' is how many days make up the "year" period you look at
 * it outputs the first date (start date of the period) and
 * the percentage gain ((lastClose - firstClose) / firstClose * 100) for each stock
 */
public class TopPerformer {

    /**
     * mapper class processes each line of the input csv file and emits:
     * key: stock symbol (e.g., "AAPL")
     * value: "date,close" (e.g., "2023-01-01,145.23")
     */
    public static class MapperQ5 extends Mapper<LongWritable, Text, Text, Text> {
        private boolean isHeader = true; // flag to skip the header row

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // skip the header row (only for the first line)
            if (isHeader) {
                isHeader = false
                return
            }

            // split the input line into parts based on commas
            String[] parts = value.toString().split(",")
            if (parts.length < 4) return

            String date = parts[0].trim()
            String symbol = parts[1].trim()
            String closeStr = parts[3].trim()
            
            try {
                // validate closing price
                Double.parseDouble(closeStr)
                // emit symbol as key, and "date,close" as value
                context.write(new Text(symbol), new Text(date + "," + closeStr))
            } catch (NumberFormatException e) {
                // skip invalid price lines
            }
        }
    }

    /**
     * reducer class to determine how each stock performed over the last 'x' days
     * 'x' is read from the config, meant to represent "the last year"
     *
     * steps:
     * - collect all date,close records for each stock
     * - parse and sort them by date
     * - find the latest date and filter out records outside 'x' days from that date
     * - if enough data remains, calculate percent gain:
     *   ((lastClose - firstClose) / firstClose) * 100
     * - output stock symbol, start date (earliest date in range), and percent gain
     */
    public static class ReducerQ5 extends Reducer<Text, Text, Text, Text> {
        private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd")

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration()
            int days = conf.getInt("x", 365) // default to 365 days if not provided

            // collect all records: [date, close]
            List<String[]> records = new ArrayList<>()
            List<Date> parsedDates = new ArrayList<>()

            for (Text val : values) {
                String[] parts = val.toString().split(",")
                if (parts.length == 2) {
                    try {
                        Date d = DATE_FORMAT.parse(parts[0])
                        records.add(parts)
                        parsedDates.add(d)
                    } catch (ParseException e) {
                        // skip invalid date
                    }
                }
            }

            // if no valid records, skip
            if (records.isEmpty()) return

            // sort records by date
            List<RecordWithDate> combined = new ArrayList<>()
            for (int i = 0; i < records.size(); i++) {
                combined.add(new RecordWithDate(parsedDates.get(i), records.get(i)))
            }
            combined.sort(Comparator.comparing(r -> r.date))

            // filter records within the last x days
            Date maxDate = combined.get(combined.size() - 1).date
            Calendar cal = Calendar.getInstance()
            cal.setTime(maxDate)
            cal.add(Calendar.DAY_OF_YEAR, -days)
            Date cutoffDate = cal.getTime()

            List<String[]> filtered = new ArrayList<>()
            for (RecordWithDate rwd : combined) {
                if (!rwd.date.before(cutoffDate)) {
                    filtered.add(rwd.record)
                }
            }

            // if fewer than 2 points, skip
            if (filtered.size() < 2) return

            // calculate percentage gain
            double firstClose = Double.parseDouble(filtered.get(0)[1])
            double lastClose = Double.parseDouble(filtered.get(filtered.size() - 1)[1])
            double percentGain = ((lastClose - firstClose) / firstClose) * 100.0

            // output symbol with start date and percent gain
            context.write(key, new Text(filtered.get(0)[0] + "," + percentGain))
        }

        // helper class to store records alongside their parsed date
        static class RecordWithDate {
            Date date
            String[] record

            RecordWithDate(Date date, String[] record) {
                this.date = date
                this.record = record
            }
        }
    }

    /**
     * the main method sets up and configures the hadoop job
     * usage: 
     *   hadoop jar "$JAR_NAME" com.example.stockanalysis.TopPerformer -Dx=365 "$HDFS_INPUT_DIR" "$HDFS_OUTPUT_DIR/trending"
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("usage: topperformer [-Dx=<days>] <input> <output>")
            System.exit(-1)
        }

        Configuration conf = new Configuration()

        Job job = Job.getInstance(conf, "topperformer")
        job.setJarByClass(TopPerformer.class)
        job.setMapperClass(MapperQ5.class)
        job.setReducerClass(ReducerQ5.class)

        // output key and value classes
        job.setOutputKeyClass(Text.class)
        job.setOutputValueClass(Text.class)

        // set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[args.length - 2]))
        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]))

        System.exit(job.waitForCompletion(true) ? 0 : 1)
    }
}
