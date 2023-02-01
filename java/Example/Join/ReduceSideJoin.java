package com.fastcampus.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReduceSideJoin extends Configured implements Tool {

    //################################
    //###         Employee         ###
    //################################ 
    public static class EmployeeMapper extends Mapper<LongWritable, Text, Text, Text> {
        //map 함수를 거친 데이터를 저장
        Text outKey = new Text(); 
        Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // emp_no, birth_date, first_name, last_name, gender, hire_date, dept_no
            String[] split = value.toString().split(","); //, 기준으로 split

            // 키값은 dept_no
            outKey.set(split[6]); 

            // value : 1    emp_no  first_name  gender
            outValue.set("1\t" + split[0] + "\t" + split[2] + "\t" + split[4]); //1은 Employment에서 왔다는것을 알려줌
            context.write(outKey, outValue);
        }
    }

    //################################
    //###        Department        ###
    //################################ 
    public static class DepartmentMapper extends Mapper<LongWritable, Text, Text, Text> {
        //map 함수를 거친 데이터를 저장
        Text outKey = new Text();
        Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            outKey.set(split[0]);
            outValue.set("0\t" + split[1]); //0은 Department에서 왔다는것을 알려줌
            context.write(outKey, outValue);
        }
    }

    //################################
    //###        ReduceJoin        ###
    //################################ 
    public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {
        Text outKey = new Text();
        Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            
            HashMap<String, String> employeeMap = new HashMap<>();//Employment 저장 자료 구조 
            String department = "";

            for (Text t : values) {
                System.out.println(key + " " + t.toString());
                String[] split = t.toString().split("\t"); //Value 값 split
                if (split[0].equals("0")) { //Department 에서 온값인지 체크 
                    department = split[1];
                } else {
                    employeeMap.put(split[1], split[2] + "\t" + split[3]); //Employment 에서온 데이터
                }
            }

            for (Map.Entry<String, String> e : employeeMap.entrySet()) {
                outKey.set(e.getKey()); //outkey set
                outValue.set(e.getValue() + "\t" + department);
                context.write(outKey, outValue);
            }
        }
    }

    //################################
    //###           Run            ###
    //################################ 
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "ReduceSideJoin");

        job.setJarByClass(ReduceSideJoin.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //멀티 Input 
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, EmployeeMapper.class); //첫번쨰 입력 args
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, DepartmentMapper.class); //두번쨰 입력 args

        FileOutputFormat.setOutputPath(job, new Path(args[2])); //3번쨰 전달 인자 출력값
        return job.waitForCompletion(true) ? 0 : 1;
    }

    //################################
    //###           Main           ###
    //################################ 
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ReduceSideJoin(), args);
        System.exit(exitCode);
    }
}
