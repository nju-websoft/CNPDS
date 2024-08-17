package nju.websoft.chinaopendataportal.Ranking;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.text.similarity.LevenshteinDistance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javafx.util.Pair;
import nju.websoft.chinaopendataportal.Model.Metadata;
import nju.websoft.chinaopendataportal.Service.MetadataService;

@Component
public class MMRTest {

    private static final double lambda = 0.5;
    private static final int N = 100;
    private static MMRTest mmrTest;

    @Autowired
    public MetadataService metadataService;

    @PostConstruct
    public void init() {
        mmrTest = this;
        mmrTest.metadataService = this.metadataService;
    }

    private List<Pair<Integer, Double>> getMMRResult(Map<Integer, Double> originalList) {

        List<Pair<Integer, Double>> result = new ArrayList<>();
        Map<Pair<Integer, Integer>, Double> scoreMap = new HashMap<>();

        while (result.size() < N && !originalList.isEmpty()) {
            double score = -Double.MAX_VALUE;
            int select = -1;
            for (Map.Entry<Integer, Double> entry : originalList.entrySet()) {
                int dataset = entry.getKey();
                double sim1 = entry.getValue();
                double sim2 = 0;
                for (Pair<Integer, Double> iter : result) {
                    double tempScore;
                    int ds1 = Math.min(dataset, iter.getKey());
                    int ds2 = Math.max(dataset, iter.getKey());
                    if (scoreMap.containsKey(new Pair<>(ds1, ds2))) {
                        tempScore = scoreMap.get(new Pair<>(ds1, ds2));
                    } else {
                        tempScore = getSim2Score(ds1, ds2);
                        scoreMap.put(new Pair<>(ds1, ds2), tempScore);
                    }
                    if (sim2 < tempScore) {
                        sim2 = tempScore;
                    }
                }
                double mmrScore = (lambda * sim1 - (1 - lambda) * sim2);
                if (score < mmrScore) {
                    score = mmrScore;
                    select = dataset;
                }
            }
            if (select == -1) {
                break;
            }
            result.add(new Pair<>(select, score));
            originalList.remove(select);
            // System.out.print(score + "\t");
        }
        // System.out.println();
        // logger.info("Complete getMMRResult");
        return result;
    }

    public List<Pair<Integer, Double>> reRankList(List<Pair<Integer, Double>> scoreList, int k) {
        List<Pair<Integer, Double>> resultList = new ArrayList<>();
        if (scoreList.size() == 0)
            return resultList;
        // logger.info("Start RerankList");
        double currMax = scoreList.get(0).getValue();
        Map<Integer, Double> list = new HashMap<>();
        for (Pair<Integer, Double> pi : scoreList) {
            if (list.size() >= k) {
                continue;
            }
            list.put(pi.getKey(), pi.getValue() / currMax);
        }
        return getMMRResult(list);
    }

    public double getSim2Score(int dataset1, int dataset2) {
        LevenshteinDistance distance = new LevenshteinDistance();
        double metaSim = 0;
        // Metadata d1 = metadataList.get(dataset1 - 1);
        // Metadata d2 = metadataList.get(dataset2 - 1);
        try {
            Metadata d1 = mmrTest.metadataService.getMetadataByDocId(dataset1);
            Metadata d2 = mmrTest.metadataService.getMetadataByDocId(dataset2);

            /// title
            String title1 = d1.title() != null ? d1.title().trim() : "";
            String title2 = d2.title() != null ? d2.title().trim() : "";
            if (title1.equals(title2)) {
                metaSim += 0.2;
            } else {
                metaSim += 0.2 * (1 - ((double) distance.apply(title1, title2))
                        / ((double) Math.max(title1.length(), title2.length())));
            }
            // desc
            String desc1 = d1.description() != null ? d1.description().trim() : "";
            String desc2 = d2.description() != null ? d2.description().trim() : "";
            if (desc1.equals(desc2)) {
                metaSim += 0.2;
            } else {
                metaSim += 0.2 * (1 - ((double) distance.apply(desc1, desc2))
                        / ((double) Math.max(desc1.length(), desc2.length())));
            }
            // province_city_department
            String province1 = d1.province().trim();
            String province2 = d2.province().trim();
            String city1 = d1.city().trim();
            String city2 = d2.city().trim();
            String department1 = d1.department() != null ? d1.department().trim() : "";
            String department2 = d2.department() != null ? d2.department().trim() : "";
            if (province1.equals(province2)) {
                metaSim += 0.2;
            }
            if (city1.equals(city2)) {
                metaSim += 0.2;

            }
            if (department1.equals(department2)) {
                metaSim += 0.2;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // // url
        // String url1 = d1.getUrl() != null ? d1.getUrl().trim() : "" ;
        // String url2 = d2.getUrl() != null ? d2.getUrl().trim() : "" ;
        // if (url1.equals(url2)) {
        // metaSim += 0.1;
        // }

        // // industry
        // String industry1 = d1.getStandardIndustry() != null ?
        // d1.getStandardIndustry().trim() : "" ;
        // String industry2 = d2.getStandardIndustry() != null ?
        // d2.getStandardIndustry().trim() : "" ;
        // if (industry1.equals(industry2)) {
        // metaSim += 0.1;
        // }

        // System.out.println(metaSim);
        return metaSim;
    }

}
