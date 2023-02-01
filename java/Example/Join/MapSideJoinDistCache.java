package com.fastcampus.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

public class MapSideJoinDistCache extends Configured implements Tool {
    public static class MapSideJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        HashMap<String, String> departmentMap = new HashMap<>();//분산캐시 HashMAP
        Text outKey = new Text();
        Text outValue = new Text();


        //################################
        //###       분산캐시로더        ###
        //################################
        //최초 1회 실행 
        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            URI[] uris = context.getCacheFiles();//분산 캐시 파일을 읽어 HashMAP 저장
            for (URI uri : uris) {
                Path path = new Path(uri.getPath());//Path 정보 
                loadDepartmentMap(path.getName());  //loadFunction Call
            }
        }
 
        private void loadDepartmentMap(String fileName) {
            String line = "";
            try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {//BufferedReader 에 파일네임을 전달한다.
                while ((line = br.readLine()) != null) {//라인을 끝까지 반복해 읽음 
                    // dept_no, dept_name
                    String[] split = line.split(",");//, 기준으로 Split Array 생성
                    departmentMap.put(split[0], split[1]);//split[0] = 번호, split[1] = Name 
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //################################
        //###           Mapper         ###
        //################################ 
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // emp_no, birth_date, first_name, last_name, gender, hire_date, dept_no
            String[] split = value.toString().split(",");//, 기준으로 Split Array 생성

            outKey.set(split[0]);//emp_no -> key
            String deparment = departmentMap.get(split[6]); //dept_no[6] 
            deparment = deparment == null ? "Not Found" : deparment;//NULL 값 예외처리
            outValue.set(split[2] + "\t" + split[4] + "\t" + deparment);//Outvalue : firstname, gender, dept_no
            context.write(outKey, outValue);
        }
    }

    //################################
    //###           Run            ###
    //################################ 
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "MapSideJoinDistCache"); //MapReduce Driver
        
        job.addCacheFile(new URI("/user/fastcampus/join/input/departments")); //분산 캐시 추가
                                                                              //앞서 업로드한 파일
        job.setJarByClass(MapSideJoinDistCache.class);

        job.setMapperClass(MapSideJoinMapper.class); //Map Class
        job.setNumReduceTasks(0); //Reduce Classs

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    //################################
    //###           Main           ###
    //################################ 
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MapSideJoinDistCache(), args);
        System.exit(exitCode);
    }
}
