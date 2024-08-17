package nju.websoft.chinaopendataportal.Controller;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.hateoas.EntityModel;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import nju.websoft.chinaopendataportal.GlobalVariances;
import nju.websoft.chinaopendataportal.Model.Metadata;
import nju.websoft.chinaopendataportal.Model.DTO.FiltersDTO;
import nju.websoft.chinaopendataportal.Model.DTO.QueryHitsDTO;
import nju.websoft.chinaopendataportal.Model.DTO.QueryResultDTO;
import nju.websoft.chinaopendataportal.Model.DTO.ResultDTO;
import nju.websoft.chinaopendataportal.Service.MetadataService;
import nju.websoft.chinaopendataportal.Service.PortalService;
import nju.websoft.chinaopendataportal.Service.PythonBackendService;
import nju.websoft.chinaopendataportal.Util.HtmlHelper;
import nju.websoft.chinaopendataportal.Util.SearchHelper;

@RestController
@RequestMapping("/apis")
public class RestSearchController {
    @Autowired
    private MetadataService metadataService;
    @Autowired
    private PortalService portalService;

    @Autowired
    private SearchHelper searchHelper;
    @Autowired
    private PythonBackendService pythonBackendService;

    @CrossOrigin(origins = "*")
    @GetMapping(value = "/filters", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<EntityModel<FiltersDTO>> filtersApi() {
        return ResponseEntity.ok(EntityModel
                .of(new FiltersDTO(metadataService.getLocations(),
                        Arrays.asList(GlobalVariances.industryFields))));
    }

    @CrossOrigin(origins = "*")
    @GetMapping(value = "/explain", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> explainApi(@RequestParam("q") String query, @RequestParam("docid") Integer docId) {
        try {
            return ResponseEntity.ok(pythonBackendService.explainRelevance(query, docId));
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.badRequest().build();
        }
    }

    @CrossOrigin(origins = "*")
    @GetMapping(value = "/search", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<EntityModel<ResultDTO>>> searchApi(@RequestParam("q") String query,
            @RequestParam(required = false, defaultValue = "全部") String province,
            @RequestParam(required = false, defaultValue = "全部") String city,
            @RequestParam(required = false, defaultValue = "全部") String industry,
            @RequestParam(required = false, defaultValue = "全部") String openness,
            @RequestParam(required = false, defaultValue = "0") Integer rerank) {
        try {
            List<Metadata> results = searchHelper.search(query, province, city, industry, openness);

            List<Metadata> finalResults = results;
            if (rerank != null && rerank > 0) {
                QueryHitsDTO hits = new QueryHitsDTO(query, IntStream.range(0, results.size())
                        .mapToObj(i -> {
                            Metadata m = results.get(i);
                            return new QueryResultDTO(0, i + 1, m.doc_id(), m.dataset_id(),
                                    String.format("%s: %s", m.title(), m.description()),
                                    Double.valueOf(results.size() - i));
                        })
                        .collect(Collectors.toList()));
                Map<Integer, Metadata> docidToMetadata = results.stream()
                        .collect(Collectors.toMap(Metadata::doc_id, Function.identity()));
                finalResults = pythonBackendService.rerankHits(hits).getHits().stream()
                        .map(h -> docidToMetadata.get(h.getDocid()))
                        .collect(Collectors.toList());
            }

            return ResponseEntity.ok(StreamSupport.stream(finalResults.spliterator(), false)
                    .map(m -> {
                        try {
                            return EntityModel.of(new ResultDTO(
                                    m.doc_id(),
                                    m.province(),
                                    m.city(),
                                    m.url(),
                                    portalService.getPortalByProvinceAndCity(
                                            m.province(), m.city()).name(),
                                    HtmlHelper.getHighlighter(query,
                                            m.title(), false, "class='server-set-highlight-title'"),
                                    HtmlHelper.getHighlighter(query,
                                            m.description(), false, "class='server-set-highlight-description'"),
                                    m.is_open(),
                                    Arrays.stream(m.tags().split(" "))
                                            .filter(s -> s.length() > 0)
                                            .toArray(String[]::new),
                                    Arrays.stream(m.data_formats().split(","))
                                            .filter(s -> s.length() > 0)
                                            .toArray(String[]::new),
                                    m.department(),
                                    m.industry(),
                                    m.publish_time(),
                                    m.update_time(),
                                    m.update_frequency(),
                                    m.data_volume(),
                                    m.standard_industry()));
                        } catch (Exception e) {
                            e.printStackTrace();
                            return null;
                        }
                    })
                    .filter(m -> m != null)
                    .collect(Collectors.toList()));
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.badRequest().build();
        }
    }
}
