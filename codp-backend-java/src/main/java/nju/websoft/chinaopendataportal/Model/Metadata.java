package nju.websoft.chinaopendataportal.Model;

import java.io.Serializable;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
@Getter
@Setter
public class Metadata implements Serializable {
    private Integer doc_id;
    private Integer dataset_id;

    private String province;
    private String city;
    private String url;

    private String title;
    private String description;
    private String is_open;
    private String tags;
    private String department;
    private String industry;
    private String publish_time;
    private String update_time;
    private String update_frequency;
    private String data_volume;
    private String data_formats;
    private String standard_industry;
}
