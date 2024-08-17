package nju.websoft.chinaopendataportal.Util;

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.Fragmenter;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleFragmenter;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;

import nju.websoft.chinaopendataportal.GlobalVariances;

public class HtmlHelper {

    private static String preTagFormat = "<span %s>";
    private static String postTag = "</span>";

    public static String getHighlighter(String query, String fieldValues)
            throws ParseException, IOException, InvalidTokenOffsetsException {
        return getHighlighter(query, fieldValues, true, "style='color:red'");
    }

    public static String getHighlighter(String query, String fieldValues, Boolean cutOff, String tagStyle)
            throws ParseException, IOException, InvalidTokenOffsetsException {

        Analyzer analyzer = GlobalVariances.globalAnalyzer;
        QueryParser queryParser = new QueryParser("title", analyzer);
        query = QueryParser.escape(query);
        Query parsedQuery = queryParser.parse(query);

        String preTag = String.format(preTagFormat, tagStyle);
        SimpleHTMLFormatter simpleHTMLFormatter = new SimpleHTMLFormatter(preTag, postTag);

        Fragmenter fragmenter = new SimpleFragmenter();
        if (cutOff) {
            fragmenter = new SimpleFragmenter(GlobalVariances.maxCharOfDescription);
        }
        Highlighter highlighter = new Highlighter(simpleHTMLFormatter, new QueryScorer(parsedQuery));
        highlighter.setTextFragmenter(fragmenter);

        TokenStream tokenStream = analyzer.tokenStream("", new StringReader(fieldValues));
        String res = highlighter.getBestFragment(tokenStream, fieldValues);
        if (res == null) {
            res = fieldValues;
            if (cutOff && res.length() > GlobalVariances.maxCharOfDescription) {
                res = res.substring(0, GlobalVariances.maxCharOfDescription - 1);
                res += "...";
            }
        } else {
            String frag = res.replaceAll(preTag, "").replaceAll(postTag, "");
            if (cutOff && frag.length() < fieldValues.length()) {
                res += "...";
            }
        }
        return res;
    }
}
