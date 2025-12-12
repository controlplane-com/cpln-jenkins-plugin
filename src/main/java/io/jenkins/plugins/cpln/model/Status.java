package io.jenkins.plugins.cpln.model;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Model class representing the status of a CPLN Workload.
 * Extended to support health monitoring and replica tracking for cleanup logic.
 */
@SuppressFBWarnings
public class Status {
    public boolean active;
    public String endpoint;
    public String canonicalEndpoint;
    
    // Health status of the workload
    public String health;
    
    // Replica counts for determining workload state
    public Integer readyReplicas;
    public Integer expectedReplicas;
    public Integer currentReplicas;
    
    // Parent ID for tracking
    public String parentId;
    
    // Last deployment details
    public String resolvedImages;
}
