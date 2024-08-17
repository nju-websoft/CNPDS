package nju.websoft.chinaopendataportal.Ranking;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.Similarity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javafx.util.Pair;
import nju.websoft.chinaopendataportal.GlobalVariances;
import nju.websoft.chinaopendataportal.Util.LuceneHelper;

@Component
public class RelevanceRanking {
    @Autowired
    private LuceneHelper luceneHelper;

    public long getTotalHits(String query, Similarity similarity, float[] weights) {
        IndexSearcher indexSearcher = luceneHelper.indexSearcher();
        long res = 0;
        String[] fields = GlobalVariances.queryFields;
        try {
            Analyzer analyzer = GlobalVariances.globalAnalyzer;
            Map<String, Float> boosts = new HashMap<>();
            for (int i = 0; i < fields.length; i++) {
                boosts.put(fields[i], weights[i]);
            }
            QueryParser queryParser = new MultiFieldQueryParser(fields, analyzer, boosts);
            query = QueryParser.escape(query);
            Query parsedQuery = queryParser.parse(query);
            indexSearcher.setSimilarity(similarity);
            TopDocs docsSearch = indexSearcher.search(parsedQuery, 1);
            res = docsSearch.totalHits.value;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }

    /**
     * 得到根据Lucene分数排序后的列表
     *
     * @param query
     * @return
     */
    public Pair<Long, List<Pair<Integer, Double>>> LuceneRanking(String query, Similarity similarity, float[] weights,
            Map<String, String> filterQuery) {
        IndexSearcher indexSearcher = luceneHelper.indexSearcher();
        long res = 0;
        String[] fields = GlobalVariances.queryFields;
        List<Pair<Integer, Double>> luceneRankingList = new ArrayList<>();
        try {
            Analyzer analyzer = GlobalVariances.globalAnalyzer;
            Map<String, Float> boosts = new HashMap<>();
            for (int i = 0; i < fields.length; i++) {
                boosts.put(fields[i], weights[i]);
            }
            QueryParser fieldQueryParser = new MultiFieldQueryParser(fields, analyzer, boosts);
            query = QueryParser.escape(query);
            Query fieldQuery = fieldQueryParser.parse(query);
            BooleanQuery.Builder finalQueryBuilder = new BooleanQuery.Builder().add(fieldQuery,
                    BooleanClause.Occur.MUST);
            for (Entry<String, String> entrySet : filterQuery.entrySet()) {
                String filterName = entrySet.getKey(), filterValue = entrySet.getValue();
                if (!filterValue.equals("全部")) {
                    Query parsedFilterQuery = new TermQuery(new Term(filterName, filterValue));
                    finalQueryBuilder.add(parsedFilterQuery, BooleanClause.Occur.FILTER);
                }
            }
            Query finalQuery = finalQueryBuilder.build();
            indexSearcher.setSimilarity(similarity);
            TopDocs docsSearch = indexSearcher.search(finalQuery, GlobalVariances.HitSize);
            ScoreDoc[] scoreDocs = docsSearch.scoreDocs;
            res = docsSearch.totalHits.value;
            for (ScoreDoc si : scoreDocs) {
                luceneRankingList.add(new Pair<>(si.doc, (double) si.score));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new Pair<>(res, luceneRankingList);
    }

    /**
     * 得到根据Lucene分数排序后的列表
     *
     * @param query
     * @return
     */
    public List<Pair<Integer, Double>> LuceneRankingList(String query, Similarity similarity, float[] weights,
            String[] fields) {
        IndexSearcher indexSearcher = luceneHelper.indexSearcher();
        List<Pair<Integer, Double>> luceneRankingList = new ArrayList<>();
        try {
            Analyzer analyzer = GlobalVariances.globalAnalyzer;
            Map<String, Float> boosts = new HashMap<>();
            for (int i = 0; i < fields.length; i++) {
                boosts.put(fields[i], weights[i]);
            }
            QueryParser queryParser = new MultiFieldQueryParser(fields, analyzer, boosts);
            query = QueryParser.escape(query);
            Query parsedQuery = queryParser.parse(query);
            indexSearcher.setSimilarity(similarity);
            TopDocs docsSearch = indexSearcher.search(parsedQuery, GlobalVariances.HitSize);
            ScoreDoc[] scoreDocs = docsSearch.scoreDocs;
            for (ScoreDoc si : scoreDocs) {
                luceneRankingList.add(new Pair<>(si.doc, (double) si.score));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return luceneRankingList;
    }
}
