package com.neurospark.nerdnudge.ranker.controller;

import com.google.gson.JsonObject;
import com.neurospark.nerdnudge.ranker.dto.LeaderBoardUserEntity;
import com.neurospark.nerdnudge.ranker.dto.UserEntity;
import com.neurospark.nerdnudge.ranker.response.ApiResponse;
import com.neurospark.nerdnudge.ranker.service.UserRankingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

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
    public ApiResponse<List<LeaderBoardUserEntity>> getLeaderBoard(@RequestParam(value = "topic") String topic,
                                                                   @RequestParam(value = "limit") int limit) {
        long startTime = System.currentTimeMillis();
        List<LeaderBoardUserEntity> leaderBoard = userRankingService.getLeaderBoard(topic, limit);
        long endTime = System.currentTimeMillis();
        return new ApiResponse<>("SUCCESS", "Leaderboard fetched successfully", leaderBoard, (endTime - startTime));
    }

    @GetMapping("/health")
    public ApiResponse<String> healthCheck() {
        return new ApiResponse<>("SUCCESS", "Health Check Pass", "SUCCESS", 0);
    }
}
