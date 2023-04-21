package edu.northeastern;


import edu.northeastern.Service.ElasticsearchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;

@SpringBootApplication
@EnableCaching
public class BigDataIndexing {

    @Autowired
    private static ElasticsearchService elasticsearchService;

    public static void main(String[] args) {
        SpringApplication.run(BigDataIndexing.class, args);
        System.out.println("Hello World!");
        elasticsearchService.addMessageQueueListener();
    }





}
