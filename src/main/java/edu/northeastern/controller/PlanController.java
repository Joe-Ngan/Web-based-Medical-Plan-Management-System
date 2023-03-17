package edu.northeastern.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import edu.northeastern.dto.ErrMessage;
import edu.northeastern.dto.IdResponse;
import edu.northeastern.excpetions.ResourceNotFoundException;
import edu.northeastern.model.LinkedPlanService;
import edu.northeastern.model.Plan;
import edu.northeastern.repository.PlanRepository;
import edu.northeastern.utils.JwtUtils;
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

@RestController
@RequestMapping("/plan")
public class PlanController {

    private PlanRepository planRepository;

    @Autowired
    private JwtUtils jwtUtils;

    private final JsonSchemaFactory factory = JsonSchemaFactory.byDefault();

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
            Plan plan = new ObjectMapper().readValue(request, Plan.class);
            String ETag = generateEtag(plan.toString());

            //store the body
            planRepository.save(plan);

            //set headers with etag
            HttpHeaders headers = new HttpHeaders();
            headers.set("ETag", ETag);

            //set response
            String objectId = plan.getObjectId();
            IdResponse response = new IdResponse(objectId);

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
            // check with header
            String ifNoneMatch = request.getHeader("If-None-Match");
            Plan cachedPlan = planRepository.findById(planId)
                    .orElseThrow(() -> new ResourceNotFoundException("Plan with id " + planId + " not found"));

            String resourceETag = generateEtag(cachedPlan.toString());

            if (ifNoneMatch != null && resourceETag!=null && ifNoneMatch.equals(resourceETag)) {
                return ResponseEntity.status(HttpStatus.NOT_MODIFIED).build();
            }

            //build etag
            String ETag = generateEtag(cachedPlan.toString());
            //set headers with etag
            HttpHeaders headers = new HttpHeaders();
            headers.set("ETag", ETag);

            if (cachedPlan.equals(null) || cachedPlan.equals(Optional.empty())) {
                return new ResponseEntity<>(new ErrMessage("Object does not exist."), HttpStatus.NOT_FOUND);
            } else {

                return new ResponseEntity<>(cachedPlan,headers, HttpStatus.OK);
            }
        }catch (ResourceNotFoundException ex){
            Map<String, String> response = new HashMap<>();
            response.put("message", "Object does not exist!");
            return new ResponseEntity<>(response,HttpStatus.NOT_FOUND);
        }
    }

    @DeleteMapping("/{planId}")
    public ResponseEntity<?> deleteById(@PathVariable String planId, @RequestHeader("Authorization") String tokenHeader){
        if(!jwtUtils.verify(tokenHeader)){
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("Invalid token");
        }
        try{
            Plan plan = planRepository.findById(planId)
                    .orElseThrow(() -> new ResourceNotFoundException("Plan with id " + planId + " not found"));
            System.out.println("we found the object id: "+plan.getObjectId());
            planRepository.deleteById(planId);
            return new ResponseEntity<>(HttpStatus.NO_CONTENT);
        }catch (ResourceNotFoundException ex){
            Map<String, String> response = new HashMap<>();
            response.put("message", "Object does not exist!");
            return new ResponseEntity<>(response,HttpStatus.NOT_FOUND);
        }
    }

    @PatchMapping("/{planId}")
    public ResponseEntity<?> patchById(@PathVariable String planId, @RequestBody String request, @RequestHeader("Authorization") String tokenHeader) throws NoSuchAlgorithmException, JsonProcessingException, ProcessingException, URISyntaxException{
        if(!jwtUtils.verify(tokenHeader)){
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("Invalid token");
        }
        //turn requestBody into JsonNode
        JsonNode requestBodyJson = new ObjectMapper().readTree(request);

        //verify the pattern match with json schema
        URL url = getClass().getResource("/patchSchema.json");
        JsonSchema schema = factory.getJsonSchema(String.valueOf(url.toURI()));;

        if(!schema.validate(requestBodyJson).isSuccess()){
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        }else{
            try{
                //generate the new linkedPlanService
                LinkedPlanService newLinkedPlanService = new ObjectMapper().readValue(request, LinkedPlanService.class);
                //delete the old Plan
                Plan plan = planRepository.findById(planId)
                        .orElseThrow(() -> new ResourceNotFoundException("Plan with id " + planId + " not found"));
                System.out.println("we found the object id: "+plan.getObjectId());
                planRepository.deleteById(planId);

                //renew the old LinkedPlanServices
                List<LinkedPlanService> oldPlanLinkedPlanServices = plan.getLinkedPlanServices();
                Iterator<LinkedPlanService> iterator = oldPlanLinkedPlanServices.iterator();
                while (iterator.hasNext()) {
                    LinkedPlanService obj = iterator.next();
                    if (obj.getObjectId().equals(newLinkedPlanService.getObjectId())) {
                        iterator.remove();
                        break;
                    }
                }
                oldPlanLinkedPlanServices.add(newLinkedPlanService);
                plan.setLinkedPlanServices(oldPlanLinkedPlanServices);

                //build etag and save the new plan
                String ETag = generateEtag(plan.toString());
                planRepository.save(plan);

                //set headers with etag
                HttpHeaders headers = new HttpHeaders();
                headers.set("ETag", ETag);

                //set response
                String objectId = plan.getObjectId();
                IdResponse response = new IdResponse(objectId);

                //return response, headers, status
                return new ResponseEntity<>(response,headers,HttpStatus.OK);
            }catch (ResourceNotFoundException ex) {
                Map<String, String> response = new HashMap<>();
                response.put("message", "Object does not exist!");
                return new ResponseEntity<>(response, HttpStatus.NOT_FOUND);
            }
        }
    }
}
