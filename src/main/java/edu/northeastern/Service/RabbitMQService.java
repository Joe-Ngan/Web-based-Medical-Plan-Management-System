package edu.northeastern.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class RabbitMQService {
    private RabbitTemplate rabbitTemplate;

    private final ElasticsearchService elasticsearchService;

    private static final Logger logger = LoggerFactory.getLogger(RabbitMQService.class);

    public RabbitMQService(RabbitTemplate rabbitTemplate, ElasticsearchService elasticsearchService) {
        this.rabbitTemplate = rabbitTemplate;
        this.elasticsearchService = elasticsearchService;
    }

    public void sendDocument(String message, String operation) {
        ObjectNode request = new ObjectMapper().createObjectNode();
        request.put(messageField, message);
        request.put(operationField, operation);
        rabbitTemplate.convertAndSend("planQueue_exchange", "planQueue_routing_key", request);
    }

    private static final String messageField = "message";
    private static final String operationField = "operation";
    private static final String operationPost = "post";
    private static final String operationDelete = "delete";
    private static final String indexName="indexplan";

    @RabbitListener(queues = {"planQueue"})
    public void consumerReceiveDocument(Object jobMessage){
        try {
            JsonNode job = new ObjectMapper().readTree((String)jobMessage);
            String message = job.get(messageField).asText();
            String operation = job.get(operationField).asText();
            logger.info("New Job Message Queue received: Operation: " + operation + ". Message:" + message);

            switch (operation){
                case operationPost:
                    String postResult = elasticsearchService.postDocument(new ObjectMapper().readTree(message), null, null, indexName);
                    logger.info("Operation "+operation +" completed with result: " + postResult);
                    break;
                case operationDelete:
                    String deleteResult = elasticsearchService.deleteDocument(message);
                    logger.info("Operation "+operation+" completed with result: " + deleteResult);
                    break;
                default:
                    throw new IOException(operation);
            }
        } catch (JsonProcessingException ex){
            logger.error("Unable to process message as JsonNode:"+ ex.getMessage());
        } catch (IOException ex) {
            logger.error("Unidentified Operation Type detected: "+ ex.getMessage());
        }
    }
}
