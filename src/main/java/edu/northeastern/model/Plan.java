package edu.northeastern.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.redis.core.RedisHash;

import java.io.Serializable;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@RedisHash("Plan")
public class Plan implements Serializable {
    @JsonProperty("planCostShares")
    private PlanCostShares planCostShares;
    @JsonProperty("linkedPlanServices")
    private List<LinkedPlanService> linkedPlanServices;
    @JsonProperty("_org")
    private String _org;
    @Id
    private String objectId;
    private String objectType;
    private String planType;
    private String creationDate;
}
