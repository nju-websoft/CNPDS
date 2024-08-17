package nju.websoft.chinaopendataportal.Model.DTO;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Getter
@Setter
public class FiltersDTO implements Serializable {
    private Map<String, List<String>> locations;
    private List<String> industries;
}
