package nju.websoft.chinaopendataportal.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.springframework.stereotype.Service;

import nju.websoft.chinaopendataportal.Model.Metadata;
import nju.websoft.chinaopendataportal.Util.LuceneHelper;

@Service
public class MetadataService {

    @Resource()
    private LuceneHelper luceneHelper;

    private int metadataCount = -1;
    private Map<String, List<String>> locations = new HashMap<>();
    private List<String> provinces = new ArrayList<>();

    public MetadataService() {
    }

    @PostConstruct
    public void init() {
        load(luceneHelper.indexMetadata());
    }

    public void load(Map<String, String> indexMetadata) {
        String ALL = "全部";
        locations.put(ALL, new ArrayList<>());
        provinces.add(ALL);
        indexMetadata.forEach((key, value) -> {
            if (key.equals("totalCount")) {
                metadataCount = Integer.parseInt(value);
                return;
            }
            List<String> citySet = new ArrayList<>();
            citySet.add(ALL);
            String[] cities = value.split(";");
            for (int i = 0; i < cities.length; i++) {
                String city = cities[i];
                if (city.length() <= 0) {
                    continue;
                }
                citySet.add(city);
            }
            for (int i = 1; i < citySet.size(); i++) {
                if (citySet.get(i).equals(key)) {
                    String city_to_swap = citySet.get(1);
                    citySet.set(1, key);
                    citySet.set(i, city_to_swap);
                }
            }
            locations.put(key, citySet);
            provinces.add(key);
        });
    }

    public int getMetadataCount() {
        return metadataCount;
    }

    public int getProvinceCount() {
        return locations.size();
    }

    public int getCityCount() {
        // avoid counting the "全部" city
        return locations.values().stream()
                .mapToInt(cities -> cities.size() > 0 ? cities.size() - 1 : 0)
                .reduce(0, (a, b) -> a + b);
    }

    public Map<String, List<String>> getLocations() {
        return locations;
    }

    public List<String> getProvinces() {
        return provinces;
    }

    public List<String> getCitiesByProvince(String province) {
        return locations.getOrDefault(province, new ArrayList<>());
    }

    public Metadata getMetadataByDocId(int docId) throws IOException {
        IndexReader indexReader = luceneHelper.indexReader();
        Metadata metadata = new Metadata();
        metadata.doc_id(docId);
        Document doc = indexReader.storedFields().document(docId);
        Map<String, String> obj = new HashMap<>();
        for (IndexableField field : doc.getFields()) {
            String key = field.name(), value = field.stringValue();
            if (obj.containsKey(key)) {
                value = String.format("%s, %s", obj.get(key), value);
            }
            obj.put(key, value);
        }
        obj.forEach((key, value) -> {
            try {
                Metadata.class.getMethod(key, String.class).invoke(metadata, value);
            } catch (NoSuchMethodException e) {
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                Metadata.class.getMethod(key, Integer.class).invoke(metadata, Integer.parseInt(value));
            } catch (NoSuchMethodException e) {
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // TODO: temporary process of `data_format` field
        String data_formats = metadata.data_formats();
        data_formats = data_formats.replaceAll("'|\\[|\\]|\\s", "");
        metadata.data_formats(data_formats);

        // TODO: temporary process of `data_format` field
        String tags = metadata.tags();
        tags = tags.replaceAll("'|\\[|\\]", "");
        tags = tags.replaceAll("\\s+", " ");
        metadata.tags(tags);

        return metadata;
    }

}
