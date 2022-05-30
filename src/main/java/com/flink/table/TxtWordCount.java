package com.flink.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

public class TxtWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment sTableEnv = StreamTableEnvironment.create(senv);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment bTableEnv = BatchTableEnvironment.create(env);

        //创建一个TableSource
        CsvTableSource  csvSource = new CsvTableSource("D:\\aa\\abc.csv",new String[]{"name",
                "age"},new TypeInformation[]{Types.STRING, Types.INT});

        DataStream<Row> cvsStreamRow=csvSource.getDataStream(senv);

        //注册一个TableSource，称为CvsTable
        Table csvTable=sTableEnv.fromDataStream(cvsStreamRow,"name,age");

//        Table csvTable = tableEnv.scan("CsvTable");
        Table csvResult = csvTable.select("name,age");
        csvResult = csvResult.addColumns("age+1 as age1");

        bTableEnv.createTemporaryView("student",csvResult);

        //执行Sql查询
        Table sqlQuery = bTableEnv.sqlQuery("select count(1),avg(age) from student");

        //把结果数据添加到CsvTableSink中
        bTableEnv.insertInto("D:\\aa\\result.csv",sqlQuery);

        env.execute("SQL-Batch");

    }

    public static class Student {
        public String name;
        public int age;

        public Student() {}

        public Student(String name, int age) {
            this.name = name;
            this.age = age;

        }

        @Override
        public String toString() {
            return "name:" + name + ",age:" + age;
        }
    }
}
