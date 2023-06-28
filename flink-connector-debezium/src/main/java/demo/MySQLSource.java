package demo;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class MySQLSource  extends RichParallelSourceFunction<MySQLSource.Student> {
    PreparedStatement ps;
    private Connection connection;
    private int id;
    private String name;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/flinkcdc_test?useUnicode=true&characterEncoding=UTF-8", "root", "root");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        String sql = "select * from student;";
        ps = this.connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) { //关闭连接和释放资源
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }


    @Override
    public void run(SourceContext<Student> ctx) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            int id = resultSet.getInt("id");
            String name = resultSet.getString("name");
            Student student = new Student(id,name);

            ctx.collect(student);
        }
    }

    @Override
    public void cancel() {

    }




    //
    class Student {
        public   int id;
        public String name;
        public Student(){

        }
        public Student(int id, String name){
            this.id = id;
            this.name = name;

        }
    }


}
