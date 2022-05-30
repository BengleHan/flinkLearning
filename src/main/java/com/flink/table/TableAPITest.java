package com.flink.table;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

public class TableAPITest {

    public static void main(String[] args)throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment sTableEnv = StreamTableEnvironment.create(senv);

        //创建一个TableSource
        CsvTableSource  csvSource = new CsvTableSource("D:\\aa\\abc.csv",new String[]{"name",
                "age"},new TypeInformation[]{Types.STRING, Types.INT});

        DataStream<Row> cvsStreamRow=csvSource.getDataStream(senv);

        //注册一个TableSource，称为CvsTable
        Table csvTable=sTableEnv.fromDataStream(cvsStreamRow,"name,age");

//        Table csvTable = tableEnv.scan("CsvTable");
        Table csvResult = csvTable.select("name,age");
         csvResult = csvResult.addColumns("age+1 as age1");


        DataStream<Student> csvStream = sTableEnv.toAppendStream(csvResult, Student.class);

        DataStream<Tuple3<String, Integer, Integer>> cvsStreamTuple= csvStream.map(new MapFunction<Student, Tuple3<String, Integer, Integer>>() {
            @Override
            public Tuple3<String, Integer, Integer> map(Student student) throws Exception {
                return new Tuple3<String, Integer, Integer>(student.name,student.age,student.age1);
            }
        });

        csvStream.print().setParallelism(1);

        cvsStreamTuple.writeAsCsv("D:\\aa\\abcd.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);;

        //执行任务
        senv.execute("csvStream");

    }

    public static class Student {
        public String name;
        public int age;

        public int age1;

        public Student() {}

        public Student(String name, int age, int age1) {
            this.name = name;
            this.age = age;
            this.age1 = age1;
        }

        @Override
        public String toString() {
            return "name:" + name + ",age:" + age+ ",age1:" + age1;
        }
    }
}
