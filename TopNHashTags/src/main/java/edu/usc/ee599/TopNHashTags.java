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
public class TopNHashTags {


    public static class MapHashTags extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {


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




            Pattern p = Pattern.compile("#(\\w+|\\W+)");
            Matcher m = p.matcher(tweet);

            while (m.find()) {
                output.collect(new Text(m.group(1)),new IntWritable(1));
            }





        }
    }



    public static class ReduceHashTags extends MapReduceBase implements Reducer<Text, IntWritable, IntWritable, Text> {

        public void reduce(Text text, Iterator<IntWritable> textIterator,
                           OutputCollector<IntWritable, Text> textIntWritableOutputCollector, Reporter reporter) throws IOException {

            int count =0;

            while(textIterator.hasNext()) {
                count +=textIterator.next().get();
            }

            textIntWritableOutputCollector.collect(new IntWritable(count),new Text(text));

        }
    }



    public static void main(String[] args) throws Exception{


        JobConf conf = new JobConf(TopNHashTags.class);
        conf.setJobName("TopNHashTags");

        conf.setOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(MapHashTags.class);
        conf.setReducerClass(ReduceHashTags.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);






    }
}
