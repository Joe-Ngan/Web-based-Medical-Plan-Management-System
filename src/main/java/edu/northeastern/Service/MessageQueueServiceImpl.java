package edu.northeastern.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.northeastern.repository.PlanRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class MessageQueueServiceImpl implements MessageQueueService{

    private static final String messageField = "message";
    private static final String operationField = "operation";
    private static final String messageQueue = "MessageQueue";

    @Autowired
    PlanRepository planRepository;

    @Override
    public void publish(String message, String operation) {
        ObjectNode objectNode = new ObjectMapper().createObjectNode();
        objectNode.put(messageField, message);
        objectNode.put(operationField, operation);
        planRepository.lpush(messageQueue, objectNode.toString());
    }
}
