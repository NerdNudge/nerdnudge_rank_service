package com.neurospark.nerdnudge.ranker.service;

import com.couchbase.client.java.query.QueryResult;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.neurospark.nerdnudge.couchbase.service.NerdPersistClient;
import com.neurospark.nerdnudge.ranker.dto.LeaderBoardUserEntity;
import com.neurospark.nerdnudge.ranker.dto.UserEntity;
import com.neurospark.nerdnudge.ranker.utils.RankingRefresherStatusCodes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class UserRankingRefresherService {
    public static String STATUS;
    private static final int pageSize = 5000;
    private static final int leaderboardSize = 150;
    public static JsonObject topicNameToTopicCodeMapping = null;
    public static JsonObject topicCodeToTopicNameMapping = null;

    private Map<String, UserEntity> userEntities;
    private Map<String, List<LeaderBoardUserEntity>> leaderBoardEntities;
    private NerdPersistClient userProfilesPersist;
    private NerdPersistClient configPersist;
    private JsonParser jsonParser = new JsonParser();

    @Value("${persist.users.bucket}")
    private String persistUsersBucketName;

    @Value("${persist.users.scope}")
    private String persistUsersScopeName;

    @Value("${persist.users.collection}")
    private String persistUsersCollectionName;
    private static final Logger logger = LoggerFactory.getLogger(UserRankingRefresherService.class);

    @Autowired
    public UserRankingRefresherService(@Qualifier("configPersist") NerdPersistClient configPersist,
                                       @Qualifier("userProfilesPersist") NerdPersistClient userProfilesPersist) {
        this.configPersist = configPersist;
        this.userProfilesPersist = userProfilesPersist;
        userEntities = new ConcurrentHashMap<>();
        leaderBoardEntities = new HashMap<>();

        topicNameToTopicCodeMapping = new JsonObject();
        topicCodeToTopicNameMapping = new JsonObject();
    }

    public UserEntity getUserEntity(String userId) {
        return userEntities.getOrDefault(userId, new UserEntity());
    }

    public List<LeaderBoardUserEntity> getLeaderBoard(String topic, int limit) {
        List<LeaderBoardUserEntity> topicLeaderBoard = leaderBoardEntities.getOrDefault(topic, new ArrayList<>());
        if(topicLeaderBoard.size() <= limit)
            return topicLeaderBoard;

        return topicLeaderBoard.subList(0, limit);
    }


    @Scheduled(fixedDelayString = "${user.ranks.refresh.frequency}")
    public void refreshAllRankings() {
        STATUS = RankingRefresherStatusCodes.REFRESHING;
        List<String> allTopics = getAllTopics();
        for(int i = 0; i < allTopics.size(); i ++) {
            String currentTopic = allTopics.get(i);
            int totalPages = getTotalPages(currentTopic);
            int topicRank = 0;
            logger.info("Fetching rankings for topic: {}, total pages: {}", currentTopic, totalPages);
            for (int k = 1; k <= totalPages; k++) {
                int offset = (k - 1) * pageSize;
                String topicRankQuery = getTopicRankQueryString(currentTopic, offset);
                logger.debug("Querying page {} for topic: {}", k, currentTopic);
                QueryResult result = userProfilesPersist.getDocumentsByQuery(topicRankQuery);
                for (com.couchbase.client.java.json.JsonObject row : result.rowsAsObject()) {
                    JsonObject thisResult = jsonParser.parse(row.toString()).getAsJsonObject();
                    topicRank++;
                    String userId = thisResult.get("userId").getAsString();
                    double score = thisResult.get("score").getAsDouble();
                    String userFullName = (thisResult.has("fullName")) ? thisResult.get("fullName").getAsString() : userId;

                    addUserEntity(userId, currentTopic, topicRank, score);

                    if(topicRank <= leaderboardSize) {
                        addToLeaderBoard(currentTopic, topicRank, userFullName, score);
                    }
                }
            }
        }

        STATUS = RankingRefresherStatusCodes.REFRESHED;
        logger.info("Finished refreshing rankings. Status: {}", STATUS);
    }

    private void addUserEntity(String userId, String topic, int topicRank, double score) {
        UserEntity userEntity = userEntities.getOrDefault(userId, new UserEntity());
        userEntity.setUserId(userId);
        if(userEntity.getTopicsRank() == null) {
            userEntity.setTopicsRank(new HashMap<>());
            userEntity.setTopicsScore(new HashMap<>());
        }

        userEntity.getTopicsRank().put(topic, topicRank);
        userEntity.getTopicsScore().put(topic, score);

        userEntities.put(userId, userEntity);
    }

    private void addToLeaderBoard(String topic, int topicRank, String userName, double score) {
        LeaderBoardUserEntity leaderBoardUserEntity = new LeaderBoardUserEntity();
        leaderBoardUserEntity.setUserId(userName);
        leaderBoardUserEntity.setRank(topicRank);
        leaderBoardUserEntity.setScore(score);

        String topicName = (topicCodeToTopicNameMapping.has(topic)) ? topicCodeToTopicNameMapping.get(topic).getAsString() : topic;
        List<LeaderBoardUserEntity> thisTopicLeaderboard = leaderBoardEntities.getOrDefault(topicName, new ArrayList<>());
        thisTopicLeaderboard.add(leaderBoardUserEntity);
        leaderBoardEntities.put(topicName, thisTopicLeaderboard);
    }


    private String getTopicRankQueryString(String topic, int offset) {
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("SELECT META().id as userId, userFullName as fullName, scores.");
        queryBuilder.append(topic);
        queryBuilder.append(" AS score");
        queryBuilder.append(" FROM `");
        queryBuilder.append(persistUsersBucketName);
        queryBuilder.append("`.`");
        queryBuilder.append(persistUsersScopeName);
        queryBuilder.append("`.`");
        queryBuilder.append(persistUsersCollectionName);
        queryBuilder.append("`");
        queryBuilder.append(" WHERE scores.");
        queryBuilder.append(topic);
        queryBuilder.append(" IS NOT NULL");
        queryBuilder.append(" ORDER BY scores.");
        queryBuilder.append(topic);
        queryBuilder.append(" DESC");
        queryBuilder.append(" LIMIT ");
        queryBuilder.append(pageSize);
        queryBuilder.append(" OFFSET ");
        queryBuilder.append(offset);

        return queryBuilder.toString();
    }

    private int getTotalPages(String topic) {
        String queryString = getCountsQuery(topic);
        QueryResult result = userProfilesPersist.getDocumentsByQuery(queryString);
        for (com.couchbase.client.java.json.JsonObject row : result.rowsAsObject()) {
            JsonObject thisResult = jsonParser.parse(row.toString()).getAsJsonObject();
            if(thisResult.has("count")) {
                int totalUsers = thisResult.get("count").getAsInt();
                logger.info("Total users for topic: {}: {}", topic, totalUsers);
                return (int) Math.ceil((double) totalUsers / pageSize);
            }
        }
        return 1;
    }

    private String getCountsQuery(String topic) {
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("SELECT COUNT(1) AS count FROM `");
        queryBuilder.append(persistUsersBucketName);
        queryBuilder.append("`.`");
        queryBuilder.append(persistUsersScopeName);
        queryBuilder.append("`.`");
        queryBuilder.append(persistUsersCollectionName);
        queryBuilder.append("` ");
        queryBuilder.append("WHERE scores.");
        queryBuilder.append(topic);
        queryBuilder.append(" IS NOT NULL");

        return queryBuilder.toString();
    }


    private List<String> getAllTopics() {
        topicNameToTopicCodeMapping = new JsonObject();
        topicCodeToTopicNameMapping = new JsonObject();
        JsonObject topicCodeToTopicNameMappingObject = configPersist.get("collection_topic_mapping");
        List<String> allTopics = new ArrayList<>();
        allTopics.add("global");

        Iterator<Map.Entry<String, JsonElement>> topicsIterator = topicCodeToTopicNameMappingObject.entrySet().iterator();
        while(topicsIterator.hasNext()) {
            Map.Entry<String, JsonElement> thisEntry = topicsIterator.next();
            allTopics.add(thisEntry.getKey());
            topicNameToTopicCodeMapping.addProperty(thisEntry.getValue().getAsString(), thisEntry.getKey());
            topicCodeToTopicNameMapping.addProperty(thisEntry.getKey(), thisEntry.getValue().getAsString());
        }

        logger.info("Mapping topicNameToTopicCodeMapping: {}", topicNameToTopicCodeMapping);
        logger.info("Mapping topicCodeToTopicNameMapping: {}", topicCodeToTopicNameMapping);
        logger.info("All Topics for which Ranks will be refreshed: {}", allTopics);
        return allTopics;
    }
}
