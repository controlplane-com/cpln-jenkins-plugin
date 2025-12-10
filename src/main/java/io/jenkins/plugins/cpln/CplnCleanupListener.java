package io.jenkins.plugins.cpln;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import hudson.Extension;
import hudson.model.TaskListener;
import hudson.slaves.ComputerListener;
import hudson.slaves.OfflineCause;
import jenkins.model.Jenkins;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static java.util.logging.Level.*;

/**
 * CplnCleanupListener handles cleanup of CPLN workloads on all computer lifecycle events.
 * 
 * This listener is designed to catch cleanup opportunities that the normal Jenkins
 * agent lifecycle might miss, including:
 * - Connection failures during handshake
 * - Abnormal disconnections (ClosedChannelException)
 * - Missing X-Remoting-Capability headers
 * - OOM kills on the agent side
 * - Fast exits
 * - WebSocket terminations
 * 
 * Unlike relying solely on Agent._terminate(), this listener:
 * - Triggers cleanup on ANY offline event, not just graceful termination
 * - Schedules delayed cleanup for transient failures
 * - Integrates with the WorkloadReconciler for orphan cleanup
 */
@Extension
@SuppressFBWarnings
public class CplnCleanupListener extends ComputerListener {

    private static final Logger LOGGER = Logger.getLogger(CplnCleanupListener.class.getName());
    
    // Delayed cleanup to handle transient failures
    private static final long CLEANUP_DELAY_SECONDS = Long.parseLong(
            System.getProperty("cpln.cleanup.delay.seconds", "30"));
    
    // Executor for delayed cleanup tasks
    private static final ScheduledExecutorService cleanupExecutor = 
            new ScheduledThreadPoolExecutor(2, r -> {
                Thread t = new Thread(r, "cpln-cleanup-executor");
                t.setDaemon(true);
                return t;
            });
    
    // Track pending cleanups to avoid duplicates
    private static final ConcurrentHashMap<String, Long> pendingCleanups = new ConcurrentHashMap<>();

    /**
     * Called when a computer comes online.
     * We use this to clear any pending cleanup tasks since the agent is now connected.
     */
    @Override
    public void onOnline(hudson.model.Computer c, TaskListener listener) throws IOException, InterruptedException {
        if (!(c instanceof Computer)) {
            return;
        }
        
        Computer computer = (Computer) c;
        Agent agent = computer.getNode();
        if (agent == null || agent.getCloud() == null) {
            return;
        }
        
        String workloadName = agent.getNodeName();
        
        // Cancel any pending cleanup since the agent came online
        pendingCleanups.remove(workloadName);
        
        LOGGER.log(INFO, "CPLN agent {0} came online, cancelled any pending cleanup", workloadName);
        
        // Update workload tracking
        WorkloadReconciler.trackWorkload(workloadName, agent.getCloud().name, null);
    }

    /**
     * Called when a computer goes offline.
     * This is where we trigger cleanup regardless of the offline cause.
     */
    @Override
    public void onOffline(@NonNull hudson.model.Computer c, OfflineCause cause) {
        if (!(c instanceof Computer)) {
            return;
        }
        
        Computer computer = (Computer) c;
        Agent agent = computer.getNode();
        if (agent == null || agent.getCloud() == null) {
            return;
        }
        
        String workloadName = agent.getNodeName();
        Cloud cloud = agent.getCloud();
        
        LOGGER.log(INFO, "CPLN agent {0} went offline. Cause: {1}", 
                new Object[]{workloadName, cause != null ? cause.toString() : "unknown"});
        
        // For unique agents, always schedule cleanup
        // For shared agents, only cleanup if the cause indicates a permanent failure
        if (cloud.getUseUniqueAgents() || isPermanentOfflineCause(cause)) {
            scheduleCleanup(workloadName, cloud, "agent went offline: " + 
                    (cause != null ? cause.toString() : "unknown cause"));
        }
    }

    /**
     * Called when a computer fails to launch.
     * Immediate cleanup since the workload may have been created but the agent never connected.
     */
    @Override
    public void onLaunchFailure(hudson.model.Computer c, TaskListener listener) throws IOException, InterruptedException {
        if (!(c instanceof Computer)) {
            return;
        }
        
        Computer computer = (Computer) c;
        Agent agent = computer.getNode();
        if (agent == null || agent.getCloud() == null) {
            return;
        }
        
        String workloadName = agent.getNodeName();
        Cloud cloud = agent.getCloud();
        
        LOGGER.log(WARNING, "CPLN agent {0} launch failed, scheduling immediate cleanup", workloadName);
        
        // Immediate cleanup on launch failure
        scheduleCleanup(workloadName, cloud, "launch failure", 0);
    }

    /**
     * Called when computer configuration is updated (including removal).
     */
    @Override
    public void onConfigurationChange() {
        // Force reconciliation on configuration changes to catch any orphaned workloads
        Jenkins jenkins = Jenkins.getInstanceOrNull();
        if (jenkins == null) return;
        
        for (hudson.slaves.Cloud cloud : jenkins.clouds) {
            if (cloud instanceof Cloud) {
                WorkloadReconciler.forceReconcile((Cloud) cloud);
            }
        }
    }

    /**
     * Schedule cleanup of a workload with default delay.
     */
    private void scheduleCleanup(String workloadName, Cloud cloud, String reason) {
        scheduleCleanup(workloadName, cloud, reason, CLEANUP_DELAY_SECONDS);
    }

    /**
     * Schedule cleanup of a workload with specified delay.
     */
    private void scheduleCleanup(String workloadName, Cloud cloud, String reason, long delaySeconds) {
        // Check if cleanup is already pending
        Long existingCleanup = pendingCleanups.get(workloadName);
        if (existingCleanup != null) {
            LOGGER.log(FINE, "Cleanup already scheduled for workload {0}", workloadName);
            return;
        }
        
        long scheduledTime = System.currentTimeMillis() + (delaySeconds * 1000);
        pendingCleanups.put(workloadName, scheduledTime);
        
        LOGGER.log(INFO, "Scheduling cleanup of workload {0} in {1} seconds. Reason: {2}",
                new Object[]{workloadName, delaySeconds, reason});
        
        cleanupExecutor.schedule(() -> {
            executeCleanup(workloadName, cloud, reason);
        }, delaySeconds, TimeUnit.SECONDS);
    }

    /**
     * Execute the actual cleanup.
     */
    private void executeCleanup(String workloadName, Cloud cloud, String reason) {
        // Check if cleanup is still needed (agent might have reconnected)
        Long scheduledTime = pendingCleanups.get(workloadName);
        if (scheduledTime == null) {
            LOGGER.log(FINE, "Cleanup cancelled for workload {0} - no longer pending", workloadName);
            return;
        }
        
        // Check if agent reconnected
        Jenkins jenkins = Jenkins.getInstanceOrNull();
        if (jenkins != null) {
            hudson.model.Computer computer = jenkins.getComputer(workloadName);
            if (computer != null && computer.isOnline()) {
                LOGGER.log(INFO, "Skipping cleanup for workload {0} - agent is back online", workloadName);
                pendingCleanups.remove(workloadName);
                return;
            }
        }
        
        LOGGER.log(INFO, "Executing cleanup for workload {0}. Reason: {1}",
                new Object[]{workloadName, reason});
        
        try {
            // Delete the workload via CPLN API
            boolean deleted = WorkloadReconciler.deleteWorkload(cloud, workloadName);
            
            if (deleted) {
                LOGGER.log(INFO, "Successfully cleaned up workload {0}", workloadName);
                
                // Also remove the Jenkins node if it still exists
                if (jenkins != null) {
                    hudson.model.Node node = jenkins.getNode(workloadName);
                    if (node != null) {
                        try {
                            jenkins.removeNode(node);
                            LOGGER.log(INFO, "Removed Jenkins node {0}", workloadName);
                        } catch (Exception e) {
                            LOGGER.log(WARNING, "Failed to remove Jenkins node {0}: {1}",
                                    new Object[]{workloadName, e.getMessage()});
                        }
                    }
                }
            } else {
                LOGGER.log(WARNING, "Failed to cleanup workload {0}, will retry via reconciler", workloadName);
            }
        } finally {
            pendingCleanups.remove(workloadName);
            WorkloadReconciler.untrackWorkload(workloadName);
        }
    }

    /**
     * Determine if an offline cause indicates a permanent failure that requires cleanup.
     */
    private boolean isPermanentOfflineCause(OfflineCause cause) {
        if (cause == null) {
            return true; // Unknown cause, be safe and clean up
        }
        
        String causeString = cause.toString().toLowerCase();
        
        // List of causes that indicate permanent failure
        return causeString.contains("channel") ||
               causeString.contains("disconnect") ||
               causeString.contains("terminated") ||
               causeString.contains("closed") ||
               causeString.contains("timeout") ||
               causeString.contains("error") ||
               causeString.contains("failed") ||
               causeString.contains("handshake") ||
               causeString.contains("oom") ||
               causeString.contains("kill") ||
               cause instanceof OfflineCause.ChannelTermination;
    }

    /**
     * Force immediate cleanup of a workload (for external callers).
     */
    public static void forceCleanup(String workloadName, Cloud cloud, String reason) {
        pendingCleanups.remove(workloadName);
        
        LOGGER.log(INFO, "Force cleanup requested for workload {0}. Reason: {1}",
                new Object[]{workloadName, reason});
        
        cleanupExecutor.execute(() -> {
            try {
                boolean deleted = WorkloadReconciler.deleteWorkload(cloud, workloadName);
                if (deleted) {
                    LOGGER.log(INFO, "Force cleanup successful for workload {0}", workloadName);
                }
            } finally {
                WorkloadReconciler.untrackWorkload(workloadName);
            }
        });
    }

    /**
     * Cancel any pending cleanup for a workload.
     */
    public static void cancelPendingCleanup(String workloadName) {
        if (pendingCleanups.remove(workloadName) != null) {
            LOGGER.log(FINE, "Cancelled pending cleanup for workload {0}", workloadName);
        }
    }

    /**
     * Check if cleanup is pending for a workload.
     */
    public static boolean isCleanupPending(String workloadName) {
        return pendingCleanups.containsKey(workloadName);
    }
}

