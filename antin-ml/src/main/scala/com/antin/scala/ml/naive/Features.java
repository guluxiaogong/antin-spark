package com.antin.scala.ml.naive;

import com.antin.java.ik.jdbc.JdbcUtil;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2017/8/4.
 * 对医生描述进行分词
 */
public class Features {

    static String path = Features.class.getClassLoader().getResource(".").toString().substring(6);

    static FileWriter fw = null;

    static int count = 1;

    public static void main(String[] args) {

        queryFeatures();

    }

    /**
     * 将分好的词写入文件中
     *
     * @param text
     * @throws Exception
     */
    public static void writeToFile(String text) throws Exception {
        if (fw == null)
            fw = new FileWriter(path + "features.txt", true);
        fw.write(text + System.lineSeparator());
        if (count > 1000) {
            fw.flush();
            count = 1;
        }
        count++;
    }

    /**
     * 从oracle中获取医生介绍
     *
     * @return
     */
    public static void queryFeatures() {

        JdbcUtil jdbcUtil = new JdbcUtil();
        String sql = "        select t1.org_id,\n" +
                "               t2.name dept_name,\n" +
                "               t3.name as s_dept_name,\n" +
                "               t3.code as s_code,\n" +
                "               t1.dept_code,\n" +
                "               t1.code,\n" +
                "               t1.name,\n" +
                "               t1.tech_title,\n" +
                "               t2.standard_dept || '-' || t1.org_id || '-' || t1.dept_code || '-' ||\n" +
                "               t1.code as doctor\n" +
                "          from urp_doctor t1\n" +
                "          left join urp_dept t2\n" +
                "            on t1.org_id = t2.org_id\n" +
                "           and t1.dept_code = t2.code\n" +
                "          left join urp_dept_standard t3\n" +
                "            on t2.standard_dept = t3.code  \n" +
                "         where t2.name is not null\n" +
                "           and t2.standard_dept is not null";
        try {
            List<Map<String, Object>> result = jdbcUtil.findResult(sql, null);
            System.out.println("......................................>"+result.size());
            result.forEach(r -> {
                try {
                    writeToFile(r.get("doctor".toUpperCase()) + " " + r.get("org_id".toUpperCase()) + " " + r.get("dept_name".toUpperCase()) + " " + r.get("s_dept_name".toUpperCase()) + " " + r.get("tech_title".toUpperCase()));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                // map.put(r.get("doctor".toUpperCase()) + "", r.get("org_id".toUpperCase()) + " " + r.get("name".toUpperCase()) + " " + r.get("s_name".toUpperCase()) + " " + r.get("tech_title".toUpperCase()));
            });

        } catch (Exception e) {

            e.printStackTrace();
        } finally {
            if (fw != null) {
                try {
                    fw.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
            }
        }
    }


}
