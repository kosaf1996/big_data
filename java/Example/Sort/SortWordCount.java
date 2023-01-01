package com.fastcampus.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SortWordCount extends Configured implements Tool {
    //################################
    //###           Mapper         ###
    //################################ 
    public static class SortMapper extends Mapper<Text, Text, LongWritable, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(Long.parseLong(value.toString())), key);//text 로 전달하면 정렬이되지않아 LongWritable 로 전달한다. 
        }
    }

    //################################
    //###           run            ###
    //################################ 
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        // 각 line을 key, val pair로 parse한다.
        // key와 value를 구분하는 delimeter는 탭으로 설정
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");//text input format key-value 형식으로 지정 

        Job job = Job.getInstance(conf, "SortWordCount"); //작업 인스턴스

        job.setJarByClass(SortWordCount.class); //필요한 정보 설정
        job.setMapperClass(SortMapper.class); // Mapper Class Call
        job.setInputFormatClass(KeyValueTextInputFormat.class); //Input Key-value 형식
        job.setOutputFormatClass(TextOutputFormat.class); 
        job.setNumReduceTasks(1); //reduce 의 task 개수 1개 1개의 리듀서에서 모든걸 처리해야 정렬 
                                 // Reduce 가 여러개이면 output 이 리듀서 만큼 나와 전체 정렬이 되지않는다. 

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    //################################
    //###           main           ###
    //################################ 
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SortWordCount(), args);
        System.exit(exitCode);
    }
}
