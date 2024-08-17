package nju.websoft.indexbuilder.utils;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.springframework.stereotype.Component;

@Component
public class IndexFactory {

    public IndexWriter indexWriter = null;
    public Integer commitCounter = 0;

    private Map<String, Set<String>> locations = new HashMap<>();
    private Map<String, String> metadata = new HashMap<String, String>();

    public void init(String storePath, Analyzer analyzer) {
        try {
            Directory directory = MMapDirectory.open(Paths.get(storePath));
            IndexWriterConfig config = new IndexWriterConfig(analyzer);
            indexWriter = new IndexWriter(directory, config);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void commitDocument(Document document) {
        try {
            indexWriter.addDocument(document);
            commitCounter++;
            if (updateLocations(document)) {
                indexWriter.setLiveCommitData(metadata.entrySet());
            }
            indexWriter.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void closeIndexWriter() {
        try {
            indexWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private boolean updateLocations(Document doc) {
        String province = doc.get("province");
        String city = doc.get("city");

        boolean changed = false;
        if (!locations.containsKey(province)) {
            changed = true;
            locations.put(province, new HashSet<>());
            metadata.put(province, "");
        }
        Set<String> cities = locations.get(province);
        if (!cities.contains(city)) {
            changed = true;
            cities.add(city);
            metadata.put(province, String.format("%s%s;", metadata.get(province), city));
        }

        return changed;
    }

    public void updateMetadata(String key, String value) {
        metadata.put(key, value);
    }
}
