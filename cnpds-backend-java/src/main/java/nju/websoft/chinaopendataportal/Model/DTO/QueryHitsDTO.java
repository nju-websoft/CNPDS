package nju.websoft.chinaopendataportal.Model.DTO;

import java.io.Serializable;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Getter
@Setter
public class QueryHitsDTO implements Serializable {
    String query;
    List<QueryResultDTO> hits;
}
