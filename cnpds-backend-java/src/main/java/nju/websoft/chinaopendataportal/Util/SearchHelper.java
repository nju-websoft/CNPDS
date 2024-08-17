package nju.websoft.chinaopendataportal.Util;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javafx.util.Pair;
import nju.websoft.chinaopendataportal.GlobalVariances;
import nju.websoft.chinaopendataportal.Model.Metadata;
import nju.websoft.chinaopendataportal.Ranking.MMRTest;
import nju.websoft.chinaopendataportal.Ranking.RelevanceRanking;
import nju.websoft.chinaopendataportal.Service.MetadataService;

@Component
public class SearchHelper {
    @Autowired
    private MetadataService metadataService;

    @Autowired
    private RelevanceRanking relevanceRanking;

    private MMRTest mmrTest = new MMRTest();

    public List<Metadata> search(String query,
            String province, String city,
            String industry, String isopen)
            throws ParseException, IOException, InvalidTokenOffsetsException {
        Map<String, String> filterMap = new HashMap<>();
        filterMap.put("province", province);
        filterMap.put("city", city);
        filterMap.put("industry", industry);
        filterMap.put("is_open", isopen);

        String queryURL = URLEncoder.encode(query, StandardCharsets.UTF_8);
        queryURL = queryURL.replaceAll("\\+", "%20");

        Pair<Long, List<Pair<Integer, Double>>> rankingResult = relevanceRanking.LuceneRanking(query,
                new BM25Similarity(), GlobalVariances.BoostWeights, filterMap);
        List<Pair<Integer, Double>> scoreList = rankingResult.getValue();
        scoreList = mmrTest.reRankList(scoreList, GlobalVariances.reRankSize);
        return StreamSupport.stream(scoreList.spliterator(), false)
                .map(pair -> {
                    try {
                        return metadataService.getMetadataByDocId(pair.getKey());
                    } catch (IOException e) {
                        e.printStackTrace();
                        return null;
                    }
                })
                .filter(pair -> pair != null)
                .collect(Collectors.toList());
    }
}
