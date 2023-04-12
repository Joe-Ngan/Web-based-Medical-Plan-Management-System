package edu.northeastern.Service;

public interface MessageQueueService {
    public void publish(String message, String operation);
}
