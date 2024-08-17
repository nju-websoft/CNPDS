package nju.websoft.chinaopendataportal.Controller;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.hateoas.EntityModel;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import nju.websoft.chinaopendataportal.Model.DTO.ActivityDTO;
import nju.websoft.chinaopendataportal.Model.DTO.StatisticsDTO;
import nju.websoft.chinaopendataportal.Service.MetadataService;
import nju.websoft.chinaopendataportal.Service.NewsService;

@RestController
@RequestMapping("/apis")
public class RestHomeController {
    @Autowired
    private MetadataService metadataService;
    @Autowired
    private NewsService newsService;

    @CrossOrigin(origins = "*")
    @GetMapping(value = "/statistics", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<EntityModel<StatisticsDTO>>> statistics() {
        List<EntityModel<StatisticsDTO>> statistics = new ArrayList<>();
        int totalCount = metadataService.getMetadataCount();
        int provinceCount = metadataService.getProvinceCount();
        int cityCount = metadataService.getCityCount();
        statistics.add(EntityModel.of(new StatisticsDTO("覆盖的省级行政区", provinceCount)));
        statistics.add(EntityModel.of(new StatisticsDTO("接入的公共数据开放平台", cityCount)));
        statistics.add(EntityModel.of(new StatisticsDTO("索引的数据集", totalCount)));

        return ResponseEntity.ok(statistics);
    }

    @CrossOrigin(origins = "*")
    @GetMapping(value = "/activities", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<ActivityDTO>> activities() {
        return ResponseEntity.ok(StreamSupport
                .stream(newsService.getTop5News().spliterator(), false)
                .map(news -> new ActivityDTO(news.title(), news.detail(), news.date()))
                .collect(Collectors.toList()));
    }
}
