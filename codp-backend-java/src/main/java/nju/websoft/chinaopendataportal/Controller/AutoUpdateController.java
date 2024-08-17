package nju.websoft.chinaopendataportal.Controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import nju.websoft.chinaopendataportal.GlobalVariances;
import nju.websoft.chinaopendataportal.Service.MetadataService;
import nju.websoft.chinaopendataportal.Util.LuceneHelper;

@RestController
@RequestMapping("/apis")
public class AutoUpdateController {

    @Autowired
    private LuceneHelper luceneHelper;

    @Autowired
    private GlobalVariances globalVariances;
    @Autowired
    private MetadataService metadataService;

    @PostMapping("/update")
    public ResponseEntity<String> update(
            @RequestParam(value = "index", required = false, defaultValue = "") String indexDir) {
        if (luceneHelper.loadIndex(indexDir)) {
            globalVariances.setIndexDir(indexDir);
            metadataService.load(luceneHelper.indexMetadata());
            return ResponseEntity.ok("Index updated successfully");
        }
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Index update failed");
    }

}
