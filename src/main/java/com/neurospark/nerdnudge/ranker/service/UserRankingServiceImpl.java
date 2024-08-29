package com.neurospark.nerdnudge.ranker.service;

import com.neurospark.nerdnudge.ranker.dto.UserEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


@Service
public class UserRankingServiceImpl implements UserRankingService {

    @Autowired
    UserRankingRefresherService userRankingRefresherService;

    @Override
    public UserEntity getUserRanksAndScores(String userId) {
        return userRankingRefresherService.getUserEntity(userId);
    }
}
