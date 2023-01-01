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
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> { //Mapper<Object, Text, Text, IntWritable>  = Mapper<Input Key class, Input value class, Output key class, Output value class >

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
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {//Reducer<Object, Text, Text, IntWritable>  = Mapper<Input Key class, Input value class, Output key class, Output value class >
        private IntWritable result = new IntWritable();
        //IntWritable : Map과 Reduce 사이에 데이터를 주고 받을 떄 네트워크를 통해 주고 받는다.
        // 객체를 네트워크를 통해 전송하면 데이터의 형태 변경되거나 다르게 해석될수 있다. 
        // 이를위해 직렬화와 역직렬화 과정이 필요하다 이를 위한 자료형을 정의한다.

        //직렬화 : 네트워크전송을위한 구조화된 객체를 바이트 스트림 으로 전환
        //역직렬화 : 바이트스트림을 구조화된 객체로 전환 

        //Reduce Function 
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException { //단어가 키가되어 같은 단어 끼리 하나의 Reduce 로 모인다. 
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
