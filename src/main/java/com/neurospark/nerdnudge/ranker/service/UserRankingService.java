package com.neurospark.nerdnudge.ranker.service;

import com.neurospark.nerdnudge.ranker.dto.UserEntity;

public interface UserRankingService {
    public UserEntity getUserRanksAndScores(String userId);
}
