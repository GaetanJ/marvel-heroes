package repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import env.ElasticConfiguration;
import env.MarvelHeroesConfiguration;
import models.Hero;
import models.PaginatedResults;
import models.SearchedHero;
import play.libs.Json;
import play.libs.ws.WSClient;
import utils.SearchedHeroSamples;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Singleton
public class ElasticRepository {

    private final WSClient wsClient;
    private final ElasticConfiguration elasticConfiguration;

    @Inject
    public ElasticRepository(WSClient wsClient, MarvelHeroesConfiguration configuration) {
        this.wsClient = wsClient;
        this.elasticConfiguration = configuration.elasticConfiguration;
    }


    public CompletionStage<PaginatedResults<SearchedHero>> searchHeroes(String input, int size, int page) {
        return wsClient.url(elasticConfiguration.uri + "/heroes/_search")
                .post(Json.parse("{\n" +
                        "    \"from\" : "+size*(page-1)+",\n" +
                        "    \"size\" : "+size+",\n" +
                        "    \"query\" : {\n" +
                        "        \"query_string\": {\n" +
                        "          \"query\": \""+input+"~\",\n" +
                        "\"fields\":  [ \"name^4\", \"aliases^3\", \"secretIdentities^3\", \"description^2\", \"partners^1\"]" +
                        "        }\n" +
                        "    }\n" +
                        "}"))
                .thenApply(response -> {
                    ArrayList<SearchedHero> heroes = new ArrayList<>();
                    JsonNode hits = Json.parse(response.getBody()).get("hits").get("hits");

                    for (int i = 0; i < hits.size(); i++) {
                        JsonNode source = hits.get(i).get("_source");
                        ((ObjectNode)source).put("id",hits.get(i).get("_id") );
                        SearchedHero shero = SearchedHero.fromJson(source);
                        heroes.add(shero);
                    }
                    return new PaginatedResults<>(heroes.size(), page, Math.round(Json.parse(response.getBody()).get("hits").get("total").get("value").asInt()/size), heroes);
                });
    }

    public CompletionStage<List<SearchedHero>> suggest(String input) {
        return wsClient.url(elasticConfiguration.uri + "/heroes/_search")
                .post(Json.parse("{\n" +
                        "    \"from\" : 0,\n" +
                        "    \"size\" : 5,\n" +
                        "    \"query\" : {\n" +
                        "        \"query_string\": {\n" +
                        "          \"query\": \""+input+"*\",\n" +
                        "\"fields\":  [ \"name^2\", \"aliases\", \"secretIdentities\"]" +
                        "        }\n" +
                        "    }\n" +
                        "}"))
                .thenApply(response -> {
                    ArrayList<SearchedHero> heroes = new ArrayList<>();
                    JsonNode hits = Json.parse(response.getBody()).get("hits").get("hits");

                    for (int i = 0; i < hits.size(); i++) {
                        JsonNode source = hits.get(i).get("_source");
                        ((ObjectNode)source).put("id",hits.get(i).get("_id") );
                        SearchedHero shero = SearchedHero.fromJson(source);
                        heroes.add(shero);
                    }
                    return heroes;
                });
    }
}
