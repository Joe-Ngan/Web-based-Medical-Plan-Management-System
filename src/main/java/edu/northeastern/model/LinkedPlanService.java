package edu.northeastern.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class LinkedPlanService implements Serializable {
    @JsonProperty("linkedService")
    private LinkedService linkedService;
    @JsonProperty("planserviceCostShares")
    private PlanserviceCostShares planserviceCostShares;
    @JsonProperty("_org")
    private String org;
    private String objectId;
    private String objectType;
}

