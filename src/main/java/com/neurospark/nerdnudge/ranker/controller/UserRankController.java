package com.neurospark.nerdnudge.ranker.controller;

import com.google.gson.JsonObject;
import com.neurospark.nerdnudge.ranker.dto.UserEntity;
import com.neurospark.nerdnudge.ranker.response.ApiResponse;
import com.neurospark.nerdnudge.ranker.service.UserRankingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/nerdnudge/userranks")
public class UserRankController {

    @Autowired
    UserRankingService userRankingService;

    @GetMapping("/getUserRanksAndScores/{id}")
    public ApiResponse<UserEntity> getUserRanksAndScores(@PathVariable(value = "id") String userId) {
        long startTime = System.currentTimeMillis();
        UserEntity userEntity = userRankingService.getUserRanksAndScores(userId);
        long endTime = System.currentTimeMillis();
        return new ApiResponse<>("SUCCESS", "User Ranks and Scores fetched successfully", userEntity, (endTime - startTime));
    }

    @GetMapping("/getLeaderBoard")
    public ApiResponse<JsonObject> getLeaderBoard(@RequestParam(value = "topic") String topic,
                                                  @RequestParam(value = "fromRank") String fromRank,
                                                  @RequestParam(value = "toRank") String toRank) {
        return null;
    }

    @GetMapping("/getUserRankTrend/{id}")
    public ApiResponse<JsonObject> getUserRankTrend(@PathVariable(value = "id") String userId, @RequestParam(value = "numDays") int numDays) {
        return null;
    }

    @GetMapping("/getUserScoreTrend/{id}")
    public ApiResponse<JsonObject> getUserScoreTrend(@PathVariable(value = "id") String userId, @RequestParam(value = "numDays") int numDays) {
        return null;
    }

    @GetMapping("/getUserRankAndScoreTrends/{id}")
    public ApiResponse<JsonObject> getUserRankAndScoreTrends(@PathVariable(value = "id") String userId) {
        return null;
    }

    @GetMapping("/getUserRankAndScoreInsights/{id}")
    public ApiResponse<JsonObject> getUserRankAndScoreInsights(@PathVariable(value = "id") String userId) {
        return null;
    }
}
