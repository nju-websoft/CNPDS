package nju.websoft.chinaopendataportal.Model.DTO;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Getter
@Setter
public class StatisticsDTO implements Serializable {
    private String title;
    private Integer count;
}
