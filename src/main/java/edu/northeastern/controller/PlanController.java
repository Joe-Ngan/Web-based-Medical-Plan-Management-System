package edu.northeastern.controller;

import edu.northeastern.Service.PlanService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/plan")
public class PlanController {

    @Autowired
    private PlanService planService;

    @PostMapping
    public ResponseEntity<?> createPlan(@RequestBody String request, @RequestHeader("Authorization") String tokenHeader) {
        return planService.post(request, tokenHeader);
    }

    @GetMapping("/{planId}")
    public ResponseEntity<?> getPlan(@PathVariable String planId, HttpServletRequest request, @RequestHeader("Authorization") String tokenHeader) {
        return planService.getById(planId, request, tokenHeader);
    }

    @DeleteMapping("/{planId}")
    public ResponseEntity<?> deletePlan(@PathVariable String planId,HttpServletRequest request, @RequestHeader("Authorization") String tokenHeader){
        return planService.deleteById(planId,request, tokenHeader);
    }

    @PatchMapping("/{planId}")
    public ResponseEntity<?> patchPlan(@PathVariable String planId, @RequestBody String requestBody,HttpServletRequest request, @RequestHeader("Authorization") String tokenHeader){
        return planService.patchById(planId, requestBody, request, tokenHeader);
    }
}
