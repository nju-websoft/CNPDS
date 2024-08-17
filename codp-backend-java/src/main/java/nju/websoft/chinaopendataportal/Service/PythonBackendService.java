package nju.websoft.chinaopendataportal.Service;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.google.gson.Gson;

import nju.websoft.chinaopendataportal.Model.DTO.QueryHitsDTO;

@Service
public class PythonBackendService {
    @Autowired
    private MetadataService metadataService;

    @Value("${websoft.chinaopendataportal.python.api}")
    private String pythonBackendUrl;

    @Value("${websoft.chinaopendataportal.python.maxreranktimes}")
    private Integer maxRerankTimes;

    private RestTemplate restTemplate = new RestTemplate();
    private Gson gson = new Gson();

    public QueryHitsDTO rerankHits(QueryHitsDTO hits) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<String> entity = new HttpEntity<>(gson.toJson(hits), headers);

        QueryHitsDTO finalHits = hits;
        for (int i = 0; i < maxRerankTimes; i++) {
            ResponseEntity<String> response = restTemplate.exchange(
                    String.format("%s/rerank", pythonBackendUrl),
                    HttpMethod.POST, entity, String.class);
            QueryHitsDTO rerankedHits = gson.fromJson(response.getBody(), QueryHitsDTO.class);
            if (rerankedHits.getHits().size() >= finalHits.getHits().size()) {
                finalHits = rerankedHits;
                break;
            }
        }

        return finalHits;
    }

    public String explainRelevance(String query, Integer docId) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        try {
            Map<String, Object> data = new HashMap<>();
            data.put("query", query);
            data.put("metadata", metadataService.getMetadataByDocId(docId));

            HttpEntity<String> entity = new HttpEntity<>(gson.toJson(data), headers);

            ResponseEntity<String> response = restTemplate.exchange(String.format("%s/explain", pythonBackendUrl),
                    HttpMethod.POST, entity,
                    String.class);
            return response.getBody();
        } catch (Exception e) {
            e.printStackTrace();
            return "Metadata Not Found";
        }
    }
}
