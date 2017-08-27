package com.antin.java.ik;

import com.antin.java.ik.jdbc.JdbcUtil;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by Administrator on 2017/8/4.
 * 对医生描述进行分词
 */
public class DoctorIntroduction {

    static String path = DoctorIntroduction.class.getClassLoader().getResource(".").toString().substring(6);

    static FileWriter fw = null;

    public static void main(String[] args) {

        HashMap<String, String> map = queryIntroduction();
        Set<Map.Entry<String, String>> set = map.entrySet();
        set.forEach(s -> {
            String content = analyzer(s.getValue());
            try {
                writeToFile(s.getKey() + "|" + content);
            } catch (Exception e) {
                if (fw != null) {
                    try {
                        fw.close();
                    } catch (IOException ioe) {
                        ioe.printStackTrace();
                    }
                }
                e.printStackTrace();
            }

        });

    }

    /**
     * 将分好的词写入文件中
     *
     * @param text
     * @throws Exception
     */
    public static void writeToFile(String text) throws Exception {
        if (fw == null)
            fw = new FileWriter(path + "introduction.txt", true);
        fw.write(text + System.lineSeparator());
        fw.flush();

    }

    /**
     * 通过IK对医生介绍分词
     *
     * @param content
     * @return
     */

    public static String analyzer(String content) {
        StringBuilder sb = new StringBuilder();
        try {
            //IKAnalyzer支持两种分词模式：最细粒度和智能分词模式，如果构造函数参数为false，那么使用最细粒度分词。
            Analyzer analyzer = new IKAnalyzer(true);
            StringReader reader = new StringReader(content);
            TokenStream ts = analyzer.tokenStream("", reader);
            CharTermAttribute term = ts.getAttribute(CharTermAttribute.class);

            while (ts.incrementToken()) {
                //System.out.print(term.toString()+" ");
                sb.append(" ");
                sb.append(term.toString());

            }
            analyzer.close();
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sb.length() > 0 ? sb.toString().substring(1) : sb.toString();
    }

    /**
     * 从oracle中获取医生介绍
     *
     * @return
     */
    public static HashMap<String, String> queryIntroduction() {
        HashMap<String, String> map = new HashMap<>();
        JdbcUtil jdbcUtil = new JdbcUtil();
        String sql = "select org_id, code, tech_title, introduction\n" +
                "  from URP_DOCTOR t\n" +
                " where introduction is not null\n" +
                "   and status = 1";
        try {
            List<Map<String, Object>> result = jdbcUtil.findResult(sql, null);
            result.forEach(r -> {
                //System.out.println(r.get("org_id".toUpperCase()) + "|" + r.get("code".toUpperCase()) + "|" + r.get("introduction".toUpperCase()) + "");
                map.put(r.get("org_id".toUpperCase()) + "|" + r.get("code".toUpperCase()) + "|" + r.get("tech_title".toUpperCase()), r.get("introduction".toUpperCase()) + "");
            });

        } catch (Exception e) {
            e.printStackTrace();
        }
        return map;
    }


}
