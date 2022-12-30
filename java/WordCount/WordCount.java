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

//################################
//###        WordCount         ###
//################################
public class WordCount extends Configured implements Tool { //Tool

    //################################
    //###       Mapper Class       ###
    //################################
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1); //Output 결과값 
        protected Text word = new Text(); //map function 결과 저장 

        //Map Function
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString()); //value Tokenizer(Text를 여러개의 Token으로 나누는 것)
            while (itr.hasMoreTokens()) { //결과를 while 문으로 반복 
                word.set(itr.nextToken().toLowerCase()); //word 객체 set
                context.write(word, one); //결과 값 : (hadoop, 1)
            }
        }
    }

    //################################
    //###      Reducer Class       ###
    //################################
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        //Reduce Function 
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0; //단어 개수 계산
            for (IntWritable val : values) {  
                sum += val.get(); 
            }

            result.set(sum); //결과 set
            context.write(key, result); //(hadoop, 3)
        }
    }

    //################################
    //###           run            ###
    //################################ 
    @Override
    public int run(String[] args) throws Exception {
        //드라이버 구현 
        Job job = Job.getInstance(getConf(), "WordCount"); //job 객체 구현 

        job.setJarByClass(WordCount.class);  //hadoop 파일을 찾아 셋팅  

        job.setMapperClass(TokenizerMapper.class); //Mapper Class Call
        job.setReducerClass(IntSumReducer.class); //Reduce Class Call

        job.setOutputKeyClass(Text.class); //Job 결과 Key Class Call
        job.setOutputValueClass(IntWritable.class); //Job 결과 value Class Call

        FileInputFormat.addInputPath(job, new Path(args[0])); //args[0]번 값 입력
        FileOutputFormat.setOutputPath(job, new Path(args[1])); //args[0]번 값 출력

        return job.waitForCompletion(true) ? 0 : 1; //정상 0 리턴 비정상 1 리턴 
    }

    //################################
    //###           main           ###
    //################################ 
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WordCount(), args); //run Function Call
        System.exit(exitCode);
    }
}
