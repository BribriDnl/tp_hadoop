package com.company;

import java.io.Console;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import sun.rmi.runtime.Log;

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{//Key_Input, Value_Input, Key_Output, Value_Output

  //INPUT: pairs of (Object, Text), that is (file or offset within a file, chunk of text to process)
  //OUTPUT: pairs of (Text, IntWritable), that is (word,1) for each word extracted

  //the words we extract from each line
	private Text word = new Text();
	//the values we will output for each word
	private final static IntWritable one = new IntWritable(1);

  //Useful for Exercise 2.2
  private Set<String> stopWords = new HashSet<>();

    //setup is called once for each Mapper instance before running the map tasks
    protected void setup(Context context) throws IOException, InterruptedException {
      //get an instance of the Configuration objects (useful to retrieve various parameters)
      Configuration conf = context.getConfiguration();
      //do something before the mappers start
      //TO DO
        String stopWordsList = conf.get("stop.words");
        StringTokenizer itr = new StringTokenizer(stopWordsList);
        while(itr.hasMoreTokens()){
            String stopWord = itr.nextToken();
            stopWords.add(stopWord);
        }
    }

    //processes one chunk of text at a time (Text value)
    //output will be written to the Context object
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      StringTokenizer itr = new StringTokenizer(value.toString());
      //splits the text into tokens separated by whitespaces
      while (itr.hasMoreTokens()) {
        String token = itr.nextToken();
        word.set(token);
        //Output pairs are collected with calls to context.write(WritableComparable, Writable).
        //in our case, we emit a key-value pair of (word, 1)
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable>{//Key_Input, Value_Input, Key_Output, Value_Output

    private IntWritable result = new IntWritable();
    private static double maxFrequency = 0;
    private static String mostFrequentWord = "";

    //sums up the values, which are the occurrence counts for each
    //key (i.e., words in this example) received from the Mappers
    //example: (word,<1,2,4,1>) -> (word,8)
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      /**
       * The computation of the most frequent word is done here.
       * To get the frequency, we can explicitly set the number of reducers to 1 s.t all words have to go through the same reducer, thus allowing to sum over all words.
       * Check by using the find function (^F) with 'job.setNumReduceTasks(1)'
       *
       * /
      if(maxFrequency < sum) {
        maxFrequency = sum;
        mostFrequentWord = key.toString();
      }
      result.set(sum);
      context.write(key, result);

    }
    //Useful for Exercise 2.3
    //cleanup is called once for each Reducer after all
    //its reduce tasks finish
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // context.write(new Text(mostFrequentWord), new IntWritable(maxFrequency));


      /** This line writes the most frequent word in the logs
       *  $HADOOP_HOME/logs/userlogs/<task_id>/<container_id>/stdout
       */
        String msg ="Most frequent word : "+mostFrequentWord+" with frequency = "+maxFrequency;
        System.out.println(msg);
        Logger.getGlobal().log(Level.INFO, msg);
    }
  }

  public static void main(String[] args) throws Exception {
    //the Configuration object for the MapReduce job
	  //you can add parameters to it using: conf.set(name,value)
    //these parameters will be visible to all Mappers and Reducers
    //who retrieve the configuration
    System.out.println(Arrays.toString(args));
	  Configuration conf = new Configuration();
    //Useful for Exercise 2.2
    conf.set("stop.words", "Lorem,ipsum,dolor");

    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);

    //

    //we indicate the class that implements the Map
    job.setMapperClass(TokenizerMapper.class);
    //the framework calls map(WritableComparable, Writable, Context)
    //for each key/value pair in the InputSplit for that task.

    //we specify a Combiner: the output of each map is passed through
    //the local Combiner (which is same as the Reducer as per the
    //job configuration) for local aggregation;
    //instead of outputing N <word,1> pairs we output <word, N>
    job.setCombinerClass(IntSumReducer.class);

    //we indicate the class that implements the Reducer
    job.setReducerClass(IntSumReducer.class);
    /**
     * Explicitly setting the number of reducers
     */
    job.setNumReduceTasks(1);
    //the framework calls reduce(WritableComparable, Iterable<Writable>, Context)
    //method for each <key, (list of values)> pair in the grouped inputs.

    //we indicate the types of the keys for this example: words -> Text
    job.setOutputKeyClass(Text.class);

    //we indicate the types of the values for this example: integers (counts) => IntWritable (a wrapper for int that is serializable and comparable)
    job.setOutputValueClass(IntWritable.class);

    //the input path passed via the command line
    //The Hadoop MapReduce framework spawns one map task for each InputSplit generated by the InputFormat for the job.
    FileInputFormat.addInputPath(job, new Path(args[1]));

    //the output path passed via the command line
    FileOutputFormat.setOutputPath(job, new Path(args[2]));

    //waitForCompletion() submits the job and monitors its progress
    System.exit(job.waitForCompletion(true) ? 0 : 1);

    //That's all, folks :)
  }
}
