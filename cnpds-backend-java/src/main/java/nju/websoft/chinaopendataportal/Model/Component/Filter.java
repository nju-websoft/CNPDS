package nju.websoft.chinaopendataportal.Model.Component;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
@Getter
@Setter
@AllArgsConstructor
public class Filter {
    String filterName;
    String currentOption;
    String paramName;
    List<String> optionList;
}
