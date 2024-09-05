package com.neurospark.nerdnudge.ranker.service;

import com.couchbase.client.java.query.QueryResult;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.neurospark.nerdnudge.couchbase.service.NerdPersistClient;
import com.neurospark.nerdnudge.ranker.dto.UserEntity;
import com.neurospark.nerdnudge.ranker.utils.RankingRefresherStatusCodes;
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

    private Map<String, UserEntity> userEntities;
    private NerdPersistClient userProfilesPersist;
    private NerdPersistClient configPersist;
    private JsonParser jsonParser = new JsonParser();

    @Value("${persist.users.bucket}")
    private String persistUsersBucketName;

    @Value("${persist.users.scope}")
    private String persistUsersScopeName;

    @Value("${persist.users.collection}")
    private String persistUsersCollectionName;

    @Autowired
    public UserRankingRefresherService(@Qualifier("configPersist") NerdPersistClient configPersist,
                                       @Qualifier("userProfilesPersist") NerdPersistClient userProfilesPersist) {
        this.configPersist = configPersist;
        this.userProfilesPersist = userProfilesPersist;
        userEntities = new ConcurrentHashMap<>();
    }

    public UserEntity getUserEntity(String userId) {
        return userEntities.getOrDefault(userId, new UserEntity());
    }


    @Scheduled(fixedDelayString = "${user.ranks.refresh.frequency}")
    public void refreshAllRankings() {
        STATUS = RankingRefresherStatusCodes.REFRESHING;
        List<String> allTopics = getAllTopics();
        for(int i = 0; i < allTopics.size(); i ++) {
            String currentTopic = allTopics.get(i);
            int totalPages = getTotalPages(currentTopic);
            int topicRank = 0;
            System.out.println(new Date() + "Fetching for topic: " + currentTopic + ", total Pages: " + totalPages);
            for (int k = 1; k <= totalPages; k++) {
                int offset = (k - 1) * pageSize;
                String topicRankQuery = getTopicRankQueryString(currentTopic, offset);
                QueryResult result = userProfilesPersist.getDocumentsByQuery(topicRankQuery);
                for (com.couchbase.client.java.json.JsonObject row : result.rowsAsObject()) {
                    JsonObject thisResult = jsonParser.parse(row.toString()).getAsJsonObject();
                    topicRank++;
                    String userId = thisResult.get("userId").getAsString();
                    double score = thisResult.get("score").getAsDouble();

                    UserEntity userEntity = userEntities.getOrDefault(userId, new UserEntity());
                    userEntity.setUserId(userId);
                    if(userEntity.getTopicsRank() == null) {
                        userEntity.setTopicsRank(new HashMap<>());
                        userEntity.setTopicsScore(new HashMap<>());
                    }

                    userEntity.getTopicsRank().put(currentTopic, topicRank);
                    userEntity.getTopicsScore().put(currentTopic, score);

                    userEntities.put(userId, userEntity);

                    if(topicRank <= 10)
                        System.out.println(userEntity);
                }
            }
        }

        STATUS = RankingRefresherStatusCodes.REFRESHED;
    }


    private String getTopicRankQueryString(String topic, int offset) {
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("SELECT META().id as userId, scores.");
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
                return (int) Math.ceil((double) thisResult.get("count").getAsInt() / pageSize);
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
        System.out.println("getting all topics.");
        JsonObject topicCodeToTopicNameMapping = configPersist.get("collection_topic_mapping");
        List<String> allTopics = new ArrayList<>();
        allTopics.add("global");

        Iterator<Map.Entry<String, JsonElement>> topicsIterator = topicCodeToTopicNameMapping.entrySet().iterator();
        while(topicsIterator.hasNext()) {
            Map.Entry<String, JsonElement> thisEntry = topicsIterator.next();
            allTopics.add(thisEntry.getKey());
        }

        return allTopics;
    }
}
