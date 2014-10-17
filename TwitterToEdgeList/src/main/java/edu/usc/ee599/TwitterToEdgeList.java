package edu.usc.ee599;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by charith on 10/5/14.
 */
public class TwitterToEdgeList {


    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {


        public void map(LongWritable longWritable, Text text, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

            String line = text.toString();
            String []parts = line.split("\t");

            if(parts.length < 7) {
                return;
            }
            String source = parts[2];

            if("".equals(source.trim())) {
                return;
            }

            String tweet = parts[6];

            if("".equals(tweet.trim())) {
                return;
            }

            String pattern = "(?<=^|(?<=[^a-zA-Z0-9-_\\\\.]))@([A-Za-z]+[A-Za-z0-9_]+)";

            Pattern p = Pattern.compile(pattern);
            Matcher m = p.matcher(tweet);

            while (m.find()) {
                String sink = m.group().substring(1);
                output.collect(new Text(source + "\t" + sink),new IntWritable(1));
            }

        }
    }


    public static class ReducerEdge extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text text, Iterator<IntWritable> intWritableIterator, OutputCollector<Text, IntWritable> textIntWritableOutputCollector, Reporter reporter) throws IOException {

            int sum = 0;
            while (intWritableIterator.hasNext()) {

                sum += intWritableIterator.next().get();
            }

            textIntWritableOutputCollector.collect(text,new IntWritable(sum));

        }
    }

    public static void main(String[] args) throws Exception{

        JobConf conf = new JobConf(TwitterToEdgeList.class);
        conf.setJobName("TwitterConverter");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(ReducerEdge.class);
        conf.setReducerClass(ReducerEdge.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);

    }
}
