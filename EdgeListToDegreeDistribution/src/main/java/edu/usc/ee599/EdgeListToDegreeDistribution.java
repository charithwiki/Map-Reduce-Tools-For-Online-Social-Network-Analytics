package edu.usc.ee599;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by charith on 10/17/14.
 */
public class EdgeListToDegreeDistribution {


    public static class MapEdges extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {


        public void map(LongWritable longWritable, Text text, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            String line = text.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);

            output.collect(new Text(tokenizer.nextToken().trim()),new Text(tokenizer.nextToken().trim()));


        }
    }


    public static class MapDegree extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {


        public void map(LongWritable longWritable, Text text, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

            String line = text.toString();

            StringTokenizer tokenizer = new StringTokenizer(line);

            String source = tokenizer.nextToken();
            String degree = tokenizer.nextToken();

            output.collect(new Text(degree.trim()),new IntWritable(1));


        }
    }


    public static class ReducerDegreeDistribution extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        public void reduce(Text text, Iterator<Text> textIterator, OutputCollector<Text, Text> textIntWritableOutputCollector, Reporter reporter) throws IOException {

            int count =0;

            while(textIterator.hasNext()) {
                textIterator.next();
                count++;
            }

            textIntWritableOutputCollector.collect(text,new Text(""+count));

        }
    }


    public static class ReducerDegreeDistributionSummmery extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {

        public void reduce(Text text, Iterator<IntWritable> intWritableIterator, OutputCollector<Text, Text> textIntWritableOutputCollector, Reporter reporter) throws IOException {

            int count =0;

            while(intWritableIterator.hasNext()) {
                count +=  intWritableIterator.next().get();
            }

            textIntWritableOutputCollector.collect(text,new Text("" + count));

        }
    }

    public static void main(String[] args) throws Exception{


        /**
         * Get degrees for each node
         */
        JobConf conf = new JobConf(EdgeListToDegreeDistribution.class);
        conf.setJobName("EdgeListToDegreeDistribution");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(MapEdges.class);
        conf.setReducerClass(ReducerDegreeDistribution.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);

        /**
         * Get Degree distribution
         */
        conf = new JobConf(EdgeListToDegreeDistribution.class);
        conf.setJobName("EdgeListToDegreeDistribution-Summery");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(MapDegree.class);   
        conf.setReducerClass(ReducerDegreeDistributionSummmery.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setMapOutputValueClass(IntWritable.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[1]));
        FileOutputFormat.setOutputPath(conf, new Path(args[2]));

        JobClient.runJob(conf);




    }
}
