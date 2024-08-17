package nju.websoft.chinaopendataportal.Model.DTO;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Getter
@Setter
public class QueryResultDTO implements Serializable {
    Integer qid;
    Integer rank;

    Integer docid;
    Integer datasetid;
    String content;
    Double score;
}
