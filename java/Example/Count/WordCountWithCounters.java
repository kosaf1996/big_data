package com.fastcampus.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


    //##########################################################################
    //###            Word에 특수 문자 포함 여부 확인 Counter 예제              ###
    //################################ #########################################
public class WordCountWithCounters extends Configured implements Tool {
    //counter 정보 
    static enum Word {
        WITHOUT_SPECIAL_CHARACTER,
        WITH_SPECIAL_CHARACTER
    }

    //################################
    //###         Mapper           ###
    //################################ 
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        //Output key, value 변수 정의
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Pattern pattern = Pattern.compile("[^a-z0-9 ]", Pattern.CASE_INSENSITIVE); //pattern 을 정규 표현식으로 특수문자 포함 여부 확인하여 함수 정보 업데이트

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());//value Tokenizer(Text를 여러개의 Token으로 나누는 것)
            while (itr.hasMoreTokens()) {
                String str = itr.nextToken().toLowerCase(); //Tokenizer 소문자로 변환하여 저장
                Matcher matcher = pattern.matcher(str); //정규표현식 패턴이 매칭되는지 체크
                if (matcher.find()) { //매칭여부에 따라 매칭이 되면 특수문자 포함 되는것이므로 WITH_SPECIAL_CHARACTER counter 
                    context.getCounter(Word.WITH_SPECIAL_CHARACTER).increment(1);
                } else { //매칭되지 않으면 WITHOUT_SPECIAL_CHARACTER counter
                    context.getCounter(Word.WITHOUT_SPECIAL_CHARACTER).increment(1);
                }
                word.set(str);
                context.write(word, one);
            }
        }
    }

    //################################
    //###          Reducer         ###
    //################################ 
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    
    //################################
    //###           run            ###
    //################################ 
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "WordCountWithCounters");

        job.setJarByClass(WordCountWithCounters.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    //################################
    //###           main           ###
    //################################ 
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WordCountWithCounters(), args);
        System.exit(exitCode);
    }
}
