package com.neurospark.nerdnudge.ranker.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;

@Data
public class UserEntity {
    private String userId;
    private Map<String, Integer> topicsRank;
    private Map<String, Double> topicsScore;
}
