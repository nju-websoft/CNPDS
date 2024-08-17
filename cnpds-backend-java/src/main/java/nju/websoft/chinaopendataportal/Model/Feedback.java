package nju.websoft.chinaopendataportal.Model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@AllArgsConstructor
@Accessors(fluent = true)
@Getter
@Setter
@Deprecated
public class Feedback {
    private String user;
    private String content;
}
