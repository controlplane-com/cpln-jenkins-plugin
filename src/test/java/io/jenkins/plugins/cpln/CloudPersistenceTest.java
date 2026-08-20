package io.jenkins.plugins.cpln;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import hudson.util.Secret;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import jenkins.model.Jenkins;
import org.junit.jupiter.api.Test;
import org.jvnet.hudson.test.JenkinsRule;
import org.jvnet.hudson.test.junit.jupiter.WithJenkins;

/**
 * Pins how a cloud survives a controller restart, i.e. the {@code config.xml}
 * round trip.
 *
 * <p>Adding {@code @Symbol} to the descriptor raises a fair question for anyone
 * already running this plugin: does it change what is written to disk, and will
 * an existing install still load? It should not — XStream marshals the cloud by
 * CLASS name, while {@code @Symbol} is descriptor metadata consumed by JCasC and
 * Job DSL for naming, and never participates in serialisation. That is a claim
 * about a mechanism though, so this test measures it instead of asserting it.
 */
@WithJenkins
class CloudPersistenceTest {

    private static Cloud sample() {
        return new Cloud(
                "persisted-cloud", "acme", "acme-gvc", "jenkins-agent", "cpln",
                true, false, 3, 300, 1024, 11, 30,
                "jenkins/inbound-agent:latest", "", "", "",
                "http://jenkins.acme.cpln.local:8080/", Secret.fromString("persisted-key"));
    }

    @Test
    void survivesASaveAndReload(JenkinsRule r) throws Exception {
        Jenkins.get().clouds.add(sample());
        Jenkins.get().save();

        // Drop the in-memory state and read the controller's own config back.
        Jenkins.get().reload();

        assertEquals(1, Jenkins.get().clouds.size(), "the cloud should survive a reload");
        Cloud reloaded = assertInstanceOf(Cloud.class, Jenkins.get().clouds.get(0));
        assertEquals("persisted-cloud", reloaded.name);
        assertEquals("acme", reloaded.getOrg());
        assertEquals("acme-gvc", reloaded.getGvc());
        assertEquals(3, reloaded.getExecutors());
        assertEquals(1024, reloaded.getMemory());
        assertEquals(11, reloaded.getRetentionMins());
        assertTrue(reloaded.getUseUniqueAgents());
        assertFalse(reloaded.getAllowJobsWithoutLabels());

        assertNotNull(reloaded.getApiKey(), "the API key should survive the round trip");
        assertEquals("persisted-key", reloaded.getApiKey().getPlainText());
    }

    /**
     * The specific reassurance for existing installs: what lands in config.xml is
     * keyed by the class name, with no trace of the symbol. An operator upgrading
     * to a build carrying {@code @Symbol} therefore reads back exactly what an
     * earlier build wrote.
     */
    @Test
    void writesTheClassNameNotTheSymbol(JenkinsRule r) throws Exception {
        Jenkins.get().clouds.add(sample());
        Jenkins.get().save();

        // getConfigFile() is protected, so read the controller's own config.xml
        // off disk — which is what an upgrading operator's Jenkins reads too.
        File config = new File(r.jenkins.getRootDir(), "config.xml");
        assertTrue(config.isFile(), "config.xml should exist at " + config);
        String configXml = Files.readString(config.toPath(), StandardCharsets.UTF_8);

        assertTrue(configXml.contains("io.jenkins.plugins.cpln.Cloud"),
                "the cloud should be persisted under its class name");
        assertFalse(configXml.contains("<cpln>"),
                "the symbol must not appear as an element in config.xml:\n" + configXml);
        assertFalse(configXml.contains("persisted-key"),
                "the API key must be encrypted at rest, never stored in the clear");
    }
}
