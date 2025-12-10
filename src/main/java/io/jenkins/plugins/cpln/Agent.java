package io.jenkins.plugins.cpln;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import hudson.Extension;
import hudson.model.Descriptor;
import hudson.model.TaskListener;
import hudson.slaves.AbstractCloudSlave;
import hudson.slaves.ComputerLauncher;
import io.jenkins.plugins.cpln.model.Workload;
import org.kohsuke.stapler.DataBoundConstructor;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.util.Objects;
import java.util.logging.Logger;

import static io.jenkins.plugins.cpln.Utils.request;
import static io.jenkins.plugins.cpln.Utils.send;
import static java.util.logging.Level.*;

/**
 * Agent implementation for CPLN workloads.
 * 
 * This class manages the lifecycle of a Jenkins agent that runs on a CPLN workload.
 * The cleanup logic has been enhanced to be independent from the remoting lifecycle:
 * 
 * 1. _terminate() handles graceful termination triggered by Jenkins retention strategy
 * 2. WorkloadReconciler handles orphan workloads (periodic background cleanup)
 * 3. CplnCleanupListener handles abnormal disconnections and connection failures
 * 
 * This ensures workloads are cleaned up regardless of:
 * - Handshake failures
 * - ClosedChannelException
 * - Missing X-Remoting-Capability headers
 * - OOM kills
 * - Fast exits
 * - Abnormal websocket terminations
 * - Jenkins controller restarts
 */
@SuppressFBWarnings
public class Agent extends AbstractCloudSlave {

    private static final long serialVersionUID = 5165504654221829569L;

    private static final Logger LOGGER = Logger.getLogger(Agent.class.getName());

    private transient Cloud cloud;
    
    // Track the actual workload name (may differ from node name)
    private String workloadName;
    
    // Track if termination has been initiated
    private transient volatile boolean terminationInitiated = false;

    public Cloud getCloud() {
        return cloud;
    }

    public void setCloud(Cloud cloud) {
        this.cloud = cloud;
    }

    /**
     * Get the CPLN workload name.
     * Falls back to node name if not explicitly set.
     */
    public String getWorkloadName() {
        return workloadName != null ? workloadName : getNodeName();
    }

    /**
     * Set the CPLN workload name.
     */
    public void setWorkloadName(String workloadName) {
        this.workloadName = workloadName;
    }

    @DataBoundConstructor
    public Agent(@NonNull String name, String remoteFS, ComputerLauncher launcher)
            throws Descriptor.FormException, IOException {
        super(name, remoteFS, launcher);
        this.workloadName = name;
    }

    @Override
    public Computer createComputer() {
        return new Computer(this);
    }

    /**
     * Terminate the agent and clean up the CPLN workload.
     * 
     * This method is called by Jenkins when:
     * - The retention strategy decides the agent should be removed
     * - The agent is manually deleted
     * - Jenkins is shutting down
     * 
     * The workload deletion is also handled by:
     * - CplnCleanupListener for abnormal disconnections
     * - WorkloadReconciler for orphan cleanup
     * 
     * This ensures multiple layers of cleanup protection.
     */
    @Override
    protected void _terminate(TaskListener listener) {
        if (terminationInitiated) {
            LOGGER.log(FINE, "Termination already initiated for agent {0}", getNodeName());
            return;
        }
        
        terminationInitiated = true;
        
        try {
            Cloud effectiveCloud = getCloud();
            if (effectiveCloud == null) {
                LOGGER.log(WARNING, "Cannot terminate agent {0} - cloud is null", getNodeName());
                // Still try to untrack
                WorkloadReconciler.untrackWorkload(getWorkloadName());
                return;
            }
            
            // Cancel any pending cleanup from listener (we're handling it here)
            CplnCleanupListener.cancelPendingCleanup(getWorkloadName());
            
            LOGGER.log(INFO, "Terminating agent {0} on cloud {1}, deleting workload {2}...",
                    new Object[]{getNodeName(), effectiveCloud, getWorkloadName()});
            
            deleteWorkloadWithRetry(effectiveCloud, getWorkloadName(), 3);
            
        } finally {
            // Always untrack the workload
            WorkloadReconciler.untrackWorkload(getWorkloadName());
        }
    }

    /**
     * Delete a workload with retry logic.
     */
    private void deleteWorkloadWithRetry(Cloud cloud, String workloadName, int maxRetries) {
        Exception lastException = null;
        
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                HttpResponse<String> response = send(request(
                        String.format(Workload.DELETEURI, cloud.getOrg(), cloud.getGvc(), workloadName),
                        SendType.DELETE, cloud.getApiKey().getPlainText()));
                
                int statusCode = response.statusCode();
                
                if (statusCode == 202 || statusCode == 200 || statusCode == 204) {
                    LOGGER.log(INFO, "Successfully deleted workload {0} on cloud {1}",
                            new Object[]{workloadName, cloud});
                    return;
                }
                
                if (statusCode == 404) {
                    LOGGER.log(INFO, "Workload {0} not found (already deleted?) on cloud {1}",
                            new Object[]{workloadName, cloud});
                    return;
                }
                
                // Log warning but continue retry
                LOGGER.log(WARNING, "Delete attempt {0} failed for workload {1}: {2} - {3}",
                        new Object[]{attempt, workloadName, statusCode, response.body()});
                
                if (attempt < maxRetries) {
                    // Wait before retry (exponential backoff)
                    Thread.sleep(1000L * attempt);
                }
                
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOGGER.log(WARNING, "Interrupted while deleting workload {0}", workloadName);
                return;
            } catch (Exception e) {
                lastException = e;
                LOGGER.log(WARNING, "Delete attempt {0} failed for workload {1}: {2}",
                        new Object[]{attempt, workloadName, e.getMessage()});
                
                if (attempt < maxRetries) {
                    try {
                        Thread.sleep(1000L * attempt);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }
        
        // All retries failed, log and let reconciler handle it
        LOGGER.log(WARNING, "All {0} attempts to delete workload {1} failed. " +
                        "WorkloadReconciler will handle cleanup. Last error: {2}",
                new Object[]{maxRetries, workloadName, 
                        lastException != null ? lastException.getMessage() : "unknown"});
    }

    /**
     * Check if termination has been initiated.
     */
    public boolean isTerminationInitiated() {
        return terminationInitiated;
    }

    /**
     * Force cleanup of the workload (for external callers).
     * This can be called when cleanup is needed outside the normal termination flow.
     */
    public void forceCleanup(String reason) {
        Cloud effectiveCloud = getCloud();
        if (effectiveCloud == null) {
            LOGGER.log(WARNING, "Cannot force cleanup for agent {0} - cloud is null", getNodeName());
            return;
        }
        
        LOGGER.log(INFO, "Force cleanup requested for agent {0}. Reason: {1}",
                new Object[]{getNodeName(), reason});
        
        CplnCleanupListener.forceCleanup(getWorkloadName(), effectiveCloud, reason);
    }

    @Extension
    @SuppressWarnings("unused")
    public static final class DescriptorImpl extends SlaveDescriptor {

        @Override
        @NonNull
        public String getDisplayName() {
            return "Cpln Agent";
        }

        @Override
        public boolean isInstantiable() {
            return false;
        }
    }
}
