package repository;

import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import models.StatItem;
import models.TopStatItem;
import play.Logger;
import utils.StatItemSamples;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

@Singleton
public class RedisRepository {

    private static Logger.ALogger logger = Logger.of("RedisRepository");


    private final RedisClient redisClient;

    @Inject
    public RedisRepository(RedisClient redisClient) {
        this.redisClient = redisClient;
    }


    public CompletionStage<Boolean> addNewHeroVisited(StatItem statItem) {
        logger.info("hero visited " + statItem.name);
        return addHeroAsLastVisited(statItem).thenCombine(incrHeroInTops(statItem), (aLong, aBoolean) -> {
            return aBoolean && aLong > 0;
        });
    }

    private CompletionStage<Boolean> incrHeroInTops(StatItem statItem) {
        StatefulRedisConnection<String, String> connection = redisClient.connect();

        RedisCommands<String, String> syncCommands = connection.sync();

        syncCommands.zaddincr("tops_heroes", 1, statItem.toJson().toString());

        connection.close();

        return CompletableFuture.completedFuture(true);
    }


    private CompletionStage<Long> addHeroAsLastVisited(StatItem statItem) {
        StatefulRedisConnection<String, String> connection = redisClient.connect();

        RedisCommands<String, String> syncCommands = connection.sync();

        syncCommands.zadd("last_heroes", new Timestamp(System.currentTimeMillis()).getTime(), statItem.toJson().toString());

        connection.close();

        return CompletableFuture.completedFuture(1L);
    }

    public CompletionStage<List<StatItem>> lastHeroesVisited(int count) {
        logger.info("Retrieved last heroes");

        StatefulRedisConnection<String, String> connection = redisClient.connect();

        return connection.async().zrevrange("last_heroes", 0, count - 1).thenApply(res ->
                res.stream().map(tuple -> StatItem.fromJson(tuple)).collect(Collectors.toList())
        );
    }

    public CompletionStage<List<TopStatItem>> topHeroesVisited(int count) {
        logger.info("Retrieved tops heroes");

        StatefulRedisConnection<String, String> connection = redisClient.connect();

        List<TopStatItem> topHeroes = new ArrayList<>();

        return connection.async().zrevrangeWithScores("tops_heroes", 0, count - 1)
                .thenApply(res ->
                        res.stream()
                                .map(tuple -> new TopStatItem(StatItem.fromJson(tuple.getValue()), Double.valueOf(tuple.getScore()).longValue()))
                                .collect(Collectors.toList())
                );

    }
}
