# Medical Plan Management System
The web-based Medical Plan Management System is a comprehensive platform that allows users to create and manage medical plans. the Medical Plan Management System is developed following industry-standard software development practices and features advanced functionalities to ensure that data integrity, security, scalability, and usability are all optimized.

## Features
* **Communication**: The RESTful APIs enable authorized users to carry out standard CRUD operations on the medical plans
* **Security**: A robust JWT token mechanism is employed to ensure that only authorized users can access and manipulate medical plans
* **Data integrity**: The system validates the request body format by utilizing JSON schema
* **Asynchronous processing**：The system enqueues any modification requests in RabbitMQ to ensure swift and efficient processing
* **Data Management**: Medical plan data is stored in a NoSQL database, Redis, allowing for rapid retrieval and management of data
* **Real-time Indexing**: Elasticsearch is utilized to ensure that the system can handle and process large amounts of data
  * Elasticsearch's parent-child relationship querying capabilities enable users to customize their query results

## Structure
* Config: rabbitmq, elasticsearch
* Controller: POST, GET, PATCH, DELETE
* Service: plan, rabbitmq, elasticsearch
* Repository: redis
* Utils: etag, json, jwt

## Dependencies
* Java 8
* Spring Boot
* redis
* RabbitMQ
* Elasticsearch
* Kibana (optional)

## Resources
* schema.json
* patchSchema.json
* mapping.json