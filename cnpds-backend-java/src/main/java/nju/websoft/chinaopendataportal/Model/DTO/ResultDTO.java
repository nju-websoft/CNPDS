package nju.websoft.chinaopendataportal.Model.DTO;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Getter
@Setter
public class ResultDTO implements Serializable {
    Integer doc_id;
    String province;
    String city;
    String url;
    String portal_name;

    String title;
    String description;
    String is_open;

    String[] tags;
    String[] data_formats;
    String department;
    String industry;
    String publish_time;
    String update_time;
    String update_frequency;
    String data_volume;
    String standard_industry;
}
