package edu.northeastern.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticsearchConfig {
    @Bean
    public RestClient restClient() {
        RestClient restClient = RestClient.builder(
                new HttpHost("localhost", 9200)).build();
        return restClient;
    }

    @Bean
    public RestHighLevelClient restHighLevelClient() {
        return new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http")));
    }

//    @Bean
//    public ElasticsearchTransport elasticsearchTransport() {
//        return new RestClientTransport(
//                restClient(), new JacksonJsonpMapper());
//    }
//
//    @Bean
//    public ElasticsearchClient elasticsearchClient(){
//        ElasticsearchClient client = new ElasticsearchClient(elasticsearchTransport());
//        return client;
//    }
}
