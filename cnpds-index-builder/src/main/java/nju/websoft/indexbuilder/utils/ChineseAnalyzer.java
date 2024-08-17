package nju.websoft.indexbuilder.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class ChineseAnalyzer {
    @Bean(name = "analyzer")
    public SmartChineseAnalyzer smartChineseAnalyzer(
            @Value("${websoft.chinaopendataportal.stopwords}") String stopwords) {
        String str;
        List<String> stopWordsList = new ArrayList<>();
        try {
            FileReader fileReader = new FileReader(stopwords);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            while ((str = bufferedReader.readLine()) != null) {
                stopWordsList.add(str);
            }
            bufferedReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        CharArraySet cas = new CharArraySet(0, true);
        cas.addAll(stopWordsList);
        return new SmartChineseAnalyzer(cas);
    }
}
