package edu.northeastern.Service;

import com.auth0.jwt.exceptions.TokenExpiredException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.northeastern.excpetions.ResourceNotFoundException;
import edu.northeastern.repository.PlanRepository;
import edu.northeastern.utils.ETagUtils;
import edu.northeastern.utils.JsonUtils;
import edu.northeastern.utils.JwtUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class PlanService {

    @Autowired
    private JwtUtils jwtUtils;

    @Autowired
    private JsonUtils jsonUtils;

    @Autowired
    private ETagUtils eTagUtils;

    @Autowired
    private RabbitMQService rabbitMQService;

    @Autowired
    private PlanRepository planRepository;

    private static final Logger logger = LoggerFactory.getLogger(PlanService.class);

    public ResponseEntity<?> post(String request, String tokenHeader) {
        try {
            if(!jwtUtils.verifyJWTToken(tokenHeader)){
                logger.error("tokenHeader is not valid: "+ tokenHeader);
                return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("Invalid token");
            }
            JsonNode requestBodyJson = new ObjectMapper().readTree(request);
            if(!jsonUtils.validateWithJsonSchema(requestBodyJson, "/schema.json")){
                logger.error("requestBody is not valid: "+ request);
                return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
            }

            String objectId = requestBodyJson.get("objectId").textValue();
            String objectType = requestBodyJson.get("objectType").textValue();
            String realId = "id_" + objectType + "_" + objectId;

            logger.info("[POST] planId: ("+realId + ") is generated in process of creating a new plan");

            planRepository.traverseInput(requestBodyJson);
            planRepository.putValue(realId, requestBodyJson.toString());
            rabbitMQService.sendDocument(request, "post");

            Map<String, String> response = new HashMap<>();
            response.put("message", "Created data with key:"+objectId);
            HttpHeaders headers = new HttpHeaders();
            String value = planRepository.getValue(realId);
            headers.set("ETag", eTagUtils.generateEtag(value));
            return new ResponseEntity<>(response,headers,HttpStatus.CREATED);
        } catch (JsonProcessingException ex){
            logger.error("Fail to parse JsonNode with the request body: "+request);
            ex.printStackTrace();
        }  catch (TokenExpiredException ex){
            logger.error("The Token has expired. "+ex.getMessage());
            ex.printStackTrace();
        }
        return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
    }



    public ResponseEntity<?> getById(String planId, HttpServletRequest request, String tokenHeader)  {
        try{
            if(!jwtUtils.verifyJWTToken(tokenHeader)){
                return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("Invalid token");
            }
            String realId = "id_"+"plan"+"_"+planId;
            String value = planRepository.getValue(realId);
            if ((value==null) || value.equals(Optional.empty())){
                throw new ResourceNotFoundException("Object does not exist.");
            }

            String ifNoneMatch = request.getHeader("If-None-Match");
            String resourceETag = eTagUtils.generateEtag(value);
            if (ifNoneMatch != null && resourceETag!=null && ifNoneMatch.equals(resourceETag)) {
                return new ResponseEntity<>(HttpStatus.NOT_MODIFIED);
            }
            JsonNode node = JsonUtils.stringToNode(value);
            planRepository.populateNestedData(node, null);

            //set headers with etag
            HttpHeaders headers = new HttpHeaders();
            String ETag = eTagUtils.generateEtag(value);
            headers.set("ETag", ETag);
            return new ResponseEntity<>(node, headers, HttpStatus.OK);
        }catch (ResourceNotFoundException ex){
            logger.error(ex.getMessage());
            Map<String, String> response = new HashMap<>();
            response.put("message", ex.getMessage());
            return new ResponseEntity<>(response,HttpStatus.NOT_FOUND);
        } catch (TokenExpiredException ex){
            logger.error("The Token has expired. "+ex.getMessage());
            ex.printStackTrace();
            return new ResponseEntity<>(ex.getMessage(),HttpStatus.NOT_FOUND);
        }
    }

    public ResponseEntity<?> deleteById(String planId, HttpServletRequest request, String tokenHeader) {
        try{
            if(!jwtUtils.verifyJWTToken(tokenHeader)){
                return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("Invalid token");
            }

            String realId = "id_"+"plan"+"_"+planId;
            String value = planRepository.getValue(realId);
            if (value.equals(null) || value.equals(Optional.empty())){
                throw new ResourceNotFoundException("Object does not exist.");
            }
            logger.info("[DELETE] planId: ("+realId + ") is fetched in process of deleting "+planId);

            if(!(eTagUtils.verifyEtag(request, value))){
                return new ResponseEntity<>("eTag is empty or not matched", HttpStatus.BAD_REQUEST);
            }

            List<String> undeleted = planRepository.deleteValueTraverse(realId);
            if(undeleted.size()>0){
                throw new ResourceNotFoundException("Objects do not exist!"+undeleted.stream().collect(Collectors.joining(", ")));
            }
            rabbitMQService.sendDocument(planId, "delete");
            return new ResponseEntity<>(HttpStatus.NO_CONTENT);
        }catch (ResourceNotFoundException ex){
            Map<String, String> response = new HashMap<>();
            response.put("message", ex.getMessage());
            return new ResponseEntity<>(response,HttpStatus.NOT_FOUND);
        }catch (NullPointerException ex){
            return new ResponseEntity<>(ex.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    public ResponseEntity<?> patchById(String planId, String requestBody, HttpServletRequest request, String tokenHeader) {
        try{
            //validate jwt
            if(!jwtUtils.verifyJWTToken(tokenHeader)){
                return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("Invalid token");
            }
            String realId = "id_"+"plan"+"_"+planId;
            String oldPlanStr = planRepository.getValue(realId);
            //validate etag
            if(!(eTagUtils.verifyEtag(request, oldPlanStr))){
                return new ResponseEntity<>("Correct eTag required before patchign. ETag is empty or not matched", HttpStatus.BAD_REQUEST);
            }
            //validate body schema
            JsonNode newNode = new ObjectMapper().readTree(requestBody);
            if(!jsonUtils.validateWithJsonSchema(newNode, "/patchSchema.json")){
                return new ResponseEntity<>("requestBody is not valid: "+ request, HttpStatus.BAD_REQUEST);
            }
            //validate plan id
            if (oldPlanStr.equals(null) || oldPlanStr.equals(Optional.empty())){
                throw new ResourceNotFoundException("Object to be patched does not exist.");
            }

            JsonNode oldPlanNode = JsonUtils.stringToNode(oldPlanStr);
            planRepository.populateNestedData(oldPlanNode, null);
            oldPlanStr = oldPlanNode.toString();

            ArrayNode existLinkedPlanServices = (ArrayNode) oldPlanNode.get("linkedPlanServices");
            existLinkedPlanServices.addAll((ArrayNode) newNode.get("linkedPlanServices"));

            //filter the repeated
            Set<JsonNode> tempNonRepeatedSet = new HashSet<>();
            Set<String> objectIds = new HashSet<>();
            for (JsonNode node : existLinkedPlanServices) {
                Iterator<Map.Entry<String, JsonNode>> iterator = node.fields();
                while (iterator.hasNext()) {
                    Map.Entry<String, JsonNode> entry = iterator.next();
                    String k = entry.getKey();
                    String v = entry.getValue().toString();
                    if (k.equals("objectId")) {//find every objectId in linkedPlanServices
                        if (!objectIds.contains(v)) {//add for the non-repeated
                            tempNonRepeatedSet.add(node);
                            objectIds.add(v);
                        }
                    }
                }
            }
            existLinkedPlanServices.removeAll();
            if (!tempNonRepeatedSet.isEmpty()){
                tempNonRepeatedSet.forEach(s -> {
                    existLinkedPlanServices.add(s);
                });
            }

            ObjectNode patchNewNode;
            if (newNode.isObject()) {
                patchNewNode = (ObjectNode) oldPlanNode;
            } else {
                patchNewNode = new ObjectMapper().convertValue(newNode, ObjectNode.class);
            }

            patchNewNode.set("linkedPlanServices", existLinkedPlanServices);
            planRepository.traverseInput(patchNewNode);
            planRepository.putValue(realId, patchNewNode.toString());
            logger.info("New Job Message Queue will be received: Operation: " + "patch" + ". Message:" + patchNewNode);
            JsonNode node = JsonUtils.stringToNode(planRepository.getValue(realId));
            planRepository.populateNestedData(node, null);
            rabbitMQService.sendDocument(node.toString(), "post");


            Map<String, String> response = new HashMap<>();
            response.put("message", "Plan with ObjectId: "+planId+" updated.");
            HttpHeaders headers = new HttpHeaders();
            String newValue = planRepository.getValue(realId);
            headers.set("ETag", eTagUtils.generateEtag(newValue));
            return new ResponseEntity<>(response,headers,HttpStatus.OK);
        }catch (ResourceNotFoundException ex) {
            ex.printStackTrace();
            return new ResponseEntity<>(ex.getMessage(), HttpStatus.NOT_FOUND);
        }catch (JsonProcessingException ex){
            logger.error("Fail to parse JsonNode with the request body: "+request);
            ex.printStackTrace();
            return new ResponseEntity<>(ex.getMessage(), HttpStatus.NOT_FOUND);
        }
    }
}
