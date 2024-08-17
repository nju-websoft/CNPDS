package nju.websoft.indexbuilder.ranking;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import nju.websoft.indexbuilder.utils.IndexFactory;

@Component
public class IndexBuilder implements CommandLineRunner {

    private Logger logger = LoggerFactory.getLogger(IndexBuilder.class);

    private JdbcTemplate jdbcTemplate;

    @Autowired
    private IndexFactory indexFactory;

    @Autowired
    private SmartChineseAnalyzer analyzer;

    @Value("${websoft.chinaopendataportal.indices.store}")
    private String storePath;

    @Value("${websoft.chinaopendataportal.table}")
    private String tableName;

    public IndexBuilder(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    private void generateDocument() throws IOException {
        int datasetCount = 0;
        List<Map<String, Object>> queryList;
        List<Integer> datasetIdList;

        FieldType fieldType = new FieldType();
        fieldType.setStored(true);
        // fieldType.setTokenized(true); // 根据field用途决定是否需要设定为tokenized
        fieldType.setStoreTermVectors(true);
        fieldType.setStoreTermVectorPositions(true);
        fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);

        String queryDatasetId = String.format("SELECT DISTINCT(dataset_id) FROM %s", tableName);
        datasetIdList = jdbcTemplate.queryForList(queryDatasetId, Integer.class);

        int totalCount = datasetIdList.size();
        logger.info("Start generating document, total: " + totalCount);
        indexFactory.updateMetadata("totalCount", String.valueOf(totalCount));

        String queryDatasetList = String.format("SELECT * FROM %s ORDER BY dataset_id", tableName);
        queryList = jdbcTemplate.queryForList(queryDatasetList);

        for (Map<String, Object> di : queryList) {
            Document document = new Document();
            for (Map.Entry<String, Object> entry : di.entrySet()) {
                String name = entry.getKey();
                String value = "";
                if (entry.getValue() != null) {
                    value = entry.getValue().toString();
                }
                if (name.equals("province") || name.equals("city")
                        || name.equals("industry") || name.equals("is_open")) {
                    fieldType.setTokenized(false);
                } else {
                    fieldType.setTokenized(true);
                }
                if (name.equals("category") || name.equals("industry")
                        || name.equals("data_formats") || name.equals("standard_industry")) {
                    String[] tags = value.split(",");
                    for (String si : tags) {
                        document.add(new Field(name, si.strip(), new FieldType(fieldType)));
                    }
                } else {
                    document.add(new Field(name, value, new FieldType(fieldType)));
                }
            }
            datasetCount++;
            // commit document
            indexFactory.commitDocument(document);
            if (datasetCount % 1000 == 0) {
                logger.info("Generated documents: " + datasetCount + "/" + totalCount);
            }
        }
        logger.info("Completed generating document, total: " + datasetCount);
    }

    public void run() throws IOException {
    }

    @Override
    public void run(String... args) throws Exception {
        indexFactory.init(storePath, analyzer);
        generateDocument();
        indexFactory.closeIndexWriter();
    }
}
