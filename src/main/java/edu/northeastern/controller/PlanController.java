package edu.northeastern.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import edu.northeastern.Service.MessageQueueService;
import edu.northeastern.Service.RedisService;
import edu.northeastern.dto.ErrMessage;
import edu.northeastern.dto.IdResponse;
import edu.northeastern.excpetions.ResourceNotFoundException;
import edu.northeastern.model.LinkedPlanService;
import edu.northeastern.model.Plan;
import edu.northeastern.repository.PlanRepository;
import edu.northeastern.utils.JsonUtils;
import edu.northeastern.utils.JwtUtils;
import lombok.val;
import org.checkerframework.checker.units.qual.A;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/plan")
public class PlanController {

    private PlanRepository planRepository;

    @Autowired
    private JwtUtils jwtUtils;

    @Autowired
    private RedisService redisService;

    @Autowired
    private MessageQueueService messageQueueService;

    private final JsonSchemaFactory factory = JsonSchemaFactory.byDefault();

    private static final Logger logger = LoggerFactory.getLogger(JwtUtils.class);

    public PlanController(PlanRepository planRepository) {
        this.planRepository = planRepository;
    }

    @PostMapping
    public ResponseEntity<?> createPlan(@RequestBody String request, @RequestHeader("Authorization") String tokenHeader) throws NoSuchAlgorithmException, JsonProcessingException, ProcessingException, URISyntaxException {
        if(!jwtUtils.verify(tokenHeader)){
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("Invalid token");
        }
        //turn requestBody into JsonNode
        JsonNode requestBodyJson = new ObjectMapper().readTree(request);

        //verify the pattern match with json schema
        URL url = getClass().getResource("/schema.json");
        JsonSchema schema = factory.getJsonSchema(String.valueOf(url.toURI()));;

        if(!schema.validate(requestBodyJson).isSuccess()){
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        }else{
            //build etag

            String objectId = requestBodyJson.get("objectId").textValue();
            String objectType = requestBodyJson.get("objectType").textValue();
            String planId = "id_" + objectType + "_" + objectId;
            logger.info("[POST] planId: ("+planId + ") is generated in process of creating a new plan");
            redisService.traverseInput(requestBodyJson);
            redisService.postValue(planId, requestBodyJson.toString());
            String value = redisService.getValue(planId);
            String ETag = generateEtag(value);

            messageQueueService.publish(request, "post");

            //set headers with etag
            HttpHeaders headers = new HttpHeaders();
            headers.set("ETag", ETag);

            //set response
            Map<String, String> response = new HashMap<>();
            response.put("message", "Created data with key:"+objectId);
            //return response, headers, status
            return new ResponseEntity<>(response,headers,HttpStatus.CREATED);
        }
    }

    private String generateEtag(String body) throws NoSuchAlgorithmException {
        byte[] hashBody = MessageDigest.getInstance("MD5").digest(body.getBytes(StandardCharsets.UTF_8));
        StringBuilder hexSb = new StringBuilder();
        for (byte b : hashBody) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1){
                hexSb.append('0');
            }
            hexSb.append(hex);
        }
        return hexSb.toString();
    }
    
    @GetMapping("/{planId}")
    public ResponseEntity<?> getById(@PathVariable String planId, HttpServletRequest request, @RequestHeader("Authorization") String tokenHeader) throws NoSuchAlgorithmException {
        if(!jwtUtils.verify(tokenHeader)){
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("Invalid token");
        }
        try{
            String realId = "id_"+"plan"+"_"+planId;
            String value = redisService.getValue(realId);
            if(value==null) throw new ResourceNotFoundException("Plan with id " + planId + " not found");
            // check with header
            String ifNoneMatch = request.getHeader("If-None-Match");
            // TODO: match exist Etag with the header Etag
            String resourceETag = generateEtag(value);

            if (ifNoneMatch != null && resourceETag!=null && ifNoneMatch.equals(resourceETag)) {
                return ResponseEntity.status(HttpStatus.NOT_MODIFIED).build();
            }
            //build etag
            String ETag = generateEtag(value);
            //set headers with etag
            HttpHeaders headers = new HttpHeaders();
            headers.set("ETag", ETag);

            if (value.equals(null) || value.equals(Optional.empty()))throw new ResourceNotFoundException("Object does not exist.");
            JsonNode node = JsonUtils.stringToNode((String)value);
            redisService.populateNestedData(node, null);
            return new ResponseEntity<>(node, headers, HttpStatus.OK);
        }catch (ResourceNotFoundException ex){
            Map<String, String> response = new HashMap<>();
            response.put("message", ex.getMessage());
            return new ResponseEntity<>(response,HttpStatus.NOT_FOUND);
        }
    }

    @DeleteMapping("/{planId}")
    public ResponseEntity<?> deleteById(@PathVariable String planId,HttpServletRequest request, @RequestHeader("Authorization") String tokenHeader){
        if(!jwtUtils.verify(tokenHeader)){
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("Invalid token");
        }
        try{
            String realId = "id_"+"plan"+"_"+planId;
            String value = redisService.getValue(realId);
            if(value!=null) logger.info("[DELETE] planId: ("+realId + ") is fetched in process of deleting "+planId);
            String etag = generateEtag(value);
            String ifMatch = request.getHeader("If-Match");
            if(ifMatch==null)return new ResponseEntity<>("eTag is required.", HttpStatus.BAD_REQUEST);
            if(etag.equals(ifMatch)){
                Set<String> childIdSet = new HashSet<>();
                childIdSet.add(realId);
                redisService.populateNestedData(JsonUtils.stringToNode(value), childIdSet);
                List<String> undeleted = new ArrayList<>();
                for(String id: childIdSet){
                    if(planRepository.deleteValue(id)==0)undeleted.add(id);
                }
                if(undeleted.size()>0)throw new ResourceNotFoundException("Objects do not exist!"+undeleted.stream().collect(Collectors.joining(", ")));
                messageQueueService.publish(planId, "delete");
                return new ResponseEntity<>(HttpStatus.NO_CONTENT);
            }else{
                return new ResponseEntity<>("eTag not matched.", HttpStatus.BAD_REQUEST);
            }
        }catch (ResourceNotFoundException ex){
            Map<String, String> response = new HashMap<>();
            response.put("message", ex.getMessage());
            return new ResponseEntity<>(response,HttpStatus.NOT_FOUND);
        }catch (NoSuchAlgorithmException ex){
            return new ResponseEntity<>("NoSuchAlgorithmException", HttpStatus.BAD_REQUEST);
        }
    }

    @PatchMapping("/{planId}")
    public ResponseEntity<?> patchById(@PathVariable String planId, @RequestBody String requestBody,HttpServletRequest request, @RequestHeader("Authorization") String tokenHeader) throws NoSuchAlgorithmException, JsonProcessingException, ProcessingException, URISyntaxException{
        if(!jwtUtils.verify(tokenHeader)){
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("Invalid token");
        }
        try{
            String realId = "id_"+"plan"+"_"+planId;
            String value = redisService.getValue(realId);
            if(value==null) throw new ResourceNotFoundException("Plan with id " + planId + " not found");

            // if-Match
            String oldEtag = generateEtag(value);
            String ifMatch = request.getHeader("If-Match");
            if(!oldEtag.equals(ifMatch)){
                return new ResponseEntity<>("eTag not valid",HttpStatus.PRECONDITION_FAILED);
            }

            JsonNode oldNode = JsonUtils.stringToNode(value);
            redisService.populateNestedData(oldNode, null);
            value = oldNode.toString();

            //turn requestBody into JsonNode
            JsonNode newNode = new ObjectMapper().readTree(requestBody);

            //verify the pattern match with json schema
            URL url = getClass().getResource("/patchSchema.json");
            JsonSchema schema = factory.getJsonSchema(String.valueOf(url.toURI()));;
            if(!schema.validate(newNode).isSuccess())return new ResponseEntity<>(HttpStatus.BAD_REQUEST);

            // add old to new (linkedPlanServices)
            ArrayNode newPlanServices = (ArrayNode) newNode.get("linkedPlanServices");
            newPlanServices.addAll((ArrayNode) oldNode.get("linkedPlanServices"));

            //filter the repeated
            Set<JsonNode> planServicesSet = new HashSet<>();
            Set<String> objectIds = new HashSet<>();
            for (JsonNode node : newPlanServices) {
                Iterator<Map.Entry<String, JsonNode>> iterator = node.fields();
                while (iterator.hasNext()) {
                    Map.Entry<String, JsonNode> entry = iterator.next();
                    String k = entry.getKey();
                    String v = entry.getValue().toString();
                    if (k.equals("objectId")) {//find every objectId in linkedPlanServices
                        if (!objectIds.contains(v)) {//add for the non-repeated
                            planServicesSet.add(node);
                            objectIds.add(v);
                        }
                    }
                }
            }

            newPlanServices.removeAll();

            if (!planServicesSet.isEmpty()){
                planServicesSet.forEach(s -> {
                    newPlanServices.add(s);
                });
            }

            redisService.traverseInput(newNode);
            redisService.postValue(realId, newNode.toString());

            //build etag with the new plan
            String newETag = generateEtag(redisService.getValue(realId));
            //set headers with etag
            HttpHeaders headers = new HttpHeaders();
            headers.set("ETag", newETag);
            //return response, headers, status
            return new ResponseEntity<>(planId+" updated.",headers,HttpStatus.OK);
        }catch (ResourceNotFoundException ex) {
            Map<String, String> response = new HashMap<>();
            response.put("message", ex.getMessage());
            return new ResponseEntity<>(response, HttpStatus.NOT_FOUND);
        }
    }
}
