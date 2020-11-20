package com.company;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

/**
 * A map/reduce program that estimates the value of Pi
 * using a quasi-Monte Carlo method.
 * <p>
 * Mapper:
 * Generate points in a unit square (numPoints)
 * and then count points inside/outside of the inscribed circle of the square.
 * <p>
 * Reducer:
 * Accumulate total points inside/outside from the mappers and computes Pi.
 * <p>
 * Usage:
 * hadoop MyPiEstimator.jar numMaps numPoints(perMap)
 * <p>
 * Let numTotal = numInside + numOutside.
 * The fraction numInside/numTotal is a rational approximation of
 * the value (Area of the circle)/(Area of the square),
 * where the area of the inscribed circle is Pi/4
 * and the area of unit square is 1.
 * Finally, the estimated value of Pi is 4(numInside/numTotal).
 * <p>
 * <p>
 * To run the program :
 * <p>
 * Firstly, make sure to have a hadoop cluster running
 * <p>
 * Secondly go to Project Structure > Artifacts,
 * Add a JAR with module dependencies, choose MyPiEstimator as the main class and tick
 * copy ... and link via manifest.
 * <p>
 * Then build the .jar executable file by going to Build > Build Artifacts
 * <p>
 * Finally use the following command to run the program :
 * hadoop jar out/artifacts/MyPiApp_jar/MyPiEstimator.jar 10 100
 */

public class MyPiEstimator {

    public static final Path tmpDir = new Path(MyPiEstimator.class.getSimpleName());
    public static final Logger logger = Logger.getLogger(MyPiEstimator.class);

    /**
     * Run a map/reduce job for estimating Pi.
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "pi");
        job.setJobName("pi");
        job.setJarByClass(MyPiEstimator.class);

        /*
         * In this example we don't process text files as Hadoop
         * does by default. Instead, each Mapper generates its own
         * input data. So we have to tell Hadoop that we want to
         * work with our own key-values for the Mappers.
         *
         * InputFormat is the class that:
         * - Selects the files or other objects that should be used for input
         * - Defines the InputSplits that break a file into tasks
         * - Provides a factory for RecordReader objects that read the file
         *
         * Several InputFormats are provided with Hadoop. An abstract type is
         * called FileInputFormat; all InputFormats that operate on files inherit
         * functionality and properties from this class. When starting a Hadoop job,
         * FileInputFormat is provided with a path containing files to read.
         * The FileInputFormat will read all files in this directory.
         * It then divides these files into one or more InputSplits each.This is the
         * default Hadoop behavior which we used in WordCount.
         *
         * You can choose which InputFormat to apply to your input files for
         * a job by calling the setInputFormat() method of the JobConf object
         * that defines the job. A table of standard InputFormats is given below.
         *  InputFormat:			Description:										Key:										Value:
         *  TextInputFormat			Default format;reads lines of text files			The byte offset of the line					The line contents
         *  KeyValueInputFormat		Parses lines into key, val pairs					Everything up to the first tab character	The remainder of the line
         *  SequenceFileInputFormat	A Hadoop-specific high-performance binary format	user-defined								user-defined
         *
         *  Since in our case we will define our own key-values for the Mapper
         *  we will use SequenceFileInputFormat
         */
        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setOutputKeyClass(BooleanWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setNumReduceTasks(1);

        //setup input/output directories
        final Path inDir = new Path(tmpDir, "in");
        final Path outDir = new Path(tmpDir, "out");
        FileInputFormat.setInputPaths(job, inDir);
        FileOutputFormat.setOutputPath(job, outDir);

        final int numMaps = Integer.parseInt(args[0]); //10
        final int numPoints = Integer.parseInt(args[1]); //100

        final FileSystem fs = FileSystem.get(conf);
        if (fs.exists(tmpDir)) {
            fs.delete(tmpDir, true);
        }
        if (!fs.mkdirs(inDir)) {
            throw new IOException("Cannot create input directory " + inDir);
        }

        /*
         * In order to control the number of mappers (which normally depends
         * on the input data size, but in this example there is no
         * actual data as each mapper generates its own points)
         * the simplest way to cheat Hadoop is to generate
         * an input file for each map task (storing the offset in the Halton sequence
         * and the number of points to generate)
         */
        for (int i = 0; i < numMaps; ++i) {
            final Path file = new Path(inDir, "part" + i);
            final LongWritable offset = new LongWritable(i * numPoints);
            final LongWritable size = new LongWritable(numPoints);
            final SequenceFile.Writer writer = SequenceFile.createWriter(
                    fs, conf, file,
                    LongWritable.class, LongWritable.class, CompressionType.NONE);
            try {
                writer.append(offset, size);
            } finally {
                writer.close();
            }
            System.out.println("Wrote input for Map #" + i);
        }

        //start a map/reduce job
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

    /**
     * Mapper class for Pi estimation.
     * Generate points in a unit square
     * and then count points inside/outside of
     * the inscribed circle of the square.
     */
    public static class MyMapper extends
            Mapper<LongWritable, LongWritable, BooleanWritable, LongWritable> {

        //INPUT: Key: offset in Hamilton; Value: number of samples to process
        //Example: (0,8) means the current map task will process the next 8 points from offset 0 in the Hamilton sequence

        //OUTPUT: Key: true (=inside) / false (=outside); Value: number of points (inside or outside, according to the key)
        //Example: (true,5) and (false,3) means 5 points were inside the circle and 3 outside

        private final BooleanWritable isInside = new BooleanWritable(); //key for output
        private final LongWritable num = new LongWritable(); //value for output

        /**
         * Map method.
         *
         * @param offset  - offset in the Halton sequence, from where to start the current processing
         * @param size    - the number of samples from the Halton sequence for this map
         * @param context - output: (true,numInside), (false,numOutside)
         */
        public void map(LongWritable offset,
                        LongWritable size,
                        Context context)
                throws IOException, InterruptedException {

            //Initialize HaltonSequence to offset
            HaltonSequence hs = new HaltonSequence(offset.get());

            //TO DO:
            //Get size points from the sequence and check whether they are inside the circle or not
            List<double[]> points = LongStream.range(0,size.get())
                    .mapToObj(i -> hs.nextPoint().clone())
                    .collect(Collectors.toList());

            // The following loop is supposed to populate my points list using the Halton Sequence
            // However  it adds the same point (which is generally the last from the Halton Sequence) size times
            /*
            for (long l = 0; l < bound; l++) {
                double[] nextPoint = hs.nextPoint();
                points.add(nextPoint);
            }
            // Expected : 100 different points
            // Actual : 100 times the last point from the Halton Sequence
            System.out.println("List values using loop");
            for (int i=0;i< size.get();i++){
                System.out.println(Arrays.toString(points.get(i)));
            }

            System.out.println("--------------------------------");

            System.out.println("List values using IntStream");
            // Tried using IntStream syntax but had the same weird output
            final HaltonSequence test = new HaltonSequence(offset.get());
            IntStream
                    .range(0, Math.toIntExact(size.get()))
                    .forEach(i -> points.add(test.nextPoint()));
            points.forEach(doubles -> System.out.println(Arrays.toString(doubles)));

            System.out.println("--------------------------------");

            System.out.println("Real values using Loop");
            // Now this loop actually prints 100 points
            for (int i = 0; i< size.get(); i++){
                System.out.println(Arrays.toString(hs.nextPoint()));
            }
            System.out.println("--------------------------------");
             */

            long insideDarts, outsideDarts;
            insideDarts = points.stream()
                    .map(doubles -> Math.pow(doubles[0] - 0.5, 2) + Math.pow(doubles[1] - 0.5, 2))
                    .filter(aDouble -> aDouble <= Math.pow(0.5, 2))
                    .count();

            outsideDarts = size.get() - insideDarts;

            //...
            System.out.println("Task : "+context.getTaskAttemptID().getTaskID()+" | insideDarts : " + insideDarts + " outsideDarts : " + outsideDarts);

            //Write to context the number of points inside the circle (true,numInside)
            //...
            context.write(new BooleanWritable(true), new LongWritable(insideDarts));

            //Write to context the number of points outside the circle (false,numOutside)
            //...
            context.write(new BooleanWritable(false), new LongWritable(outsideDarts));

        }
    }

    /**
     * Reducer class for Pi estimation.
     * Accumulate points inside/outside from the mappers
     * and then, at the end, computes Pi.
     */
    public static class MyReducer extends
            Reducer<BooleanWritable, LongWritable, WritableComparable<?>, Writable> {
        private long insideDarts = 0;
        private long outsideDarts = 0;

        //INPUT: Key: true (=inside) / false (=outside); Value: number of points (inside or outside, according to the key)
        //OUTPUT: no need to output any (key,value) pair, just print the computed Pi

        /**
         * @param isInside Are the points inside?
         * @param values   An iterator to a list of point counts
         * @param context  dummy, not used here.
         */
        public void reduce(BooleanWritable isInside,
                           Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            //TO DO:
            //Sums all individual mappers count of inside/outside points


            if (isInside.get()) {
                values.forEach(longWritable -> insideDarts += longWritable.get());
            } else {
                values.forEach(longWritable -> outsideDarts += longWritable.get());
            }

            System.out.println("insideDarts : " + insideDarts);
            System.out.println("outsideDarts : " + outsideDarts);

        }

        // When reduce tasks are over, compute and print pi estimation
        public void cleanup(Context context) throws IOException, InterruptedException {
            //TO DO:
            //compute Pi
            double pi;
            pi = 4 * (double)(insideDarts) / (double)(insideDarts + outsideDarts);

            try {
                logger.info(context.getCurrentKey());
                logger.info(context.getCurrentValue());
                System.out.println("key : " + context.getCurrentKey());
                System.out.println("value : " + context.getCurrentValue());
                System.out.println("insideDarts : " + insideDarts);
                System.out.println("outsideDarts : " + outsideDarts);
            } finally {
                //stdout for Mapper and Reducer classes is not the console
                //but it is available in the reducer logs:
                //$HADOOP_HOME/logs/userlogs/application_<job_id>/container_<task_id>/stdout
                String msg = "Estimated value of Pi is : " + pi;
                System.out.println(msg);
                logger.info(msg);
            }

        }

    }
}
