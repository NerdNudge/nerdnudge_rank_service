package com.neurospark.nerdnudge.ranker.dto;

import lombok.Data;

@Data
public class LeaderBoardUserEntity {
    String userId;
    int rank;
    double score;
}
