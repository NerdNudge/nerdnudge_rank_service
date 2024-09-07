package com.neurospark.nerdnudge.ranker.service;

import com.neurospark.nerdnudge.ranker.dto.LeaderBoardUserEntity;
import com.neurospark.nerdnudge.ranker.dto.UserEntity;

import java.util.List;

public interface UserRankingService {
    public UserEntity getUserRanksAndScores(String userId);

    public List<LeaderBoardUserEntity> getLeaderBoard(String topic, int limit);
}
