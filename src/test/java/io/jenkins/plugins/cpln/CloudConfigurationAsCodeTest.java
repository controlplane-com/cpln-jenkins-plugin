package io.jenkins.plugins.cpln;

import static io.jenkins.plugins.casc.misc.Util.getJenkinsRoot;
import static io.jenkins.plugins.casc.misc.Util.toYamlString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.jenkins.plugins.casc.ConfigurationContext;
import io.jenkins.plugins.casc.ConfiguratorRegistry;
import io.jenkins.plugins.casc.misc.ConfiguredWithCode;
import io.jenkins.plugins.casc.misc.JenkinsConfiguredWithCodeRule;
import io.jenkins.plugins.casc.misc.junit.jupiter.WithJenkinsConfiguredWithCode;
import io.jenkins.plugins.casc.model.CNode;
import jenkins.model.Jenkins;
import org.junit.jupiter.api.Test;

/**
 * Pins the Configuration as Code surface of {@link Cloud}.
 *
 * <p>This exists because the surface is easy to break invisibly. Without
 * {@code @Symbol} on the descriptor, JCasC cannot name this cloud: it refuses a
 * {@code jenkins.clouds} entry it does not recognise, and its own export writes
 * the cloud under an EMPTY key, so the configuration does not round-trip in
 * either direction. Nothing in the plugin fails to compile when that happens —
 * the failure only shows up as a controller that will not boot, a long way from
 * the cause. Hence a test rather than a comment.
 */
@WithJenkinsConfiguredWithCode
class CloudConfigurationAsCodeTest {

    @Test
    @ConfiguredWithCode("cloud-casc.yaml")
    void configuresACloudFromYaml(JenkinsConfiguredWithCodeRule r) {
        assertEquals(1, Jenkins.get().clouds.size(), "exactly one cloud should be configured");

        hudson.slaves.Cloud raw = Jenkins.get().clouds.get(0);
        Cloud cloud = assertInstanceOf(Cloud.class, raw, "the configured cloud should be the Control Plane cloud");

        // Every field, because JCasC binds by name and a rename would silently
        // leave a default in place rather than failing.
        assertEquals("test-cloud", cloud.name);
        assertEquals("test-org", cloud.getOrg());
        assertEquals("test-gvc", cloud.getGvc());
        assertEquals("jenkins-agent", cloud.getAgentWorkload());
        assertEquals("cpln linux", cloud.getLabels());
        assertTrue(cloud.getUseUniqueAgents());
        assertFalse(cloud.getAllowJobsWithoutLabels());
        assertEquals(2, cloud.getExecutors());
        assertEquals(250, cloud.getCpu());
        assertEquals(768, cloud.getMemory());
        assertEquals(7, cloud.getRetentionMins());
        assertEquals(45, cloud.getProvisioningCooldownSecs());
        assertEquals("jenkins/inbound-agent:latest", cloud.getAgentImage());
        assertEquals("http://jenkins.example.cpln.local:8080/", cloud.getJenkinsControllerUrl());

        // The API key is a Secret. It must arrive intact and must not be stored
        // as the literal placeholder text of an unresolved reference.
        assertNotNull(cloud.getApiKey(), "apiKey should be set");
        assertEquals("test-api-key", cloud.getApiKey().getPlainText());
    }

    /**
     * The regression that actually bites: export must name the cloud. Before
     * {@code @Symbol} was added this emitted {@code ? '' :} — a mapping under an
     * empty key — which reads as valid YAML and is silently unusable as input.
     */
    @Test
    @ConfiguredWithCode("cloud-casc.yaml")
    void exportsTheCloudUnderItsSymbol(JenkinsConfiguredWithCodeRule r) throws Exception {
        ConfiguratorRegistry registry = ConfiguratorRegistry.get();
        ConfigurationContext context = new ConfigurationContext(registry);
        CNode clouds = getJenkinsRoot(context).get("clouds");
        String exported = toYamlString(clouds);

        assertTrue(exported.contains("cpln:"), "export should name the cloud `cpln:`, got:\n" + exported);
        assertFalse(exported.contains("? ''"), "export should not use an empty key, got:\n" + exported);
        assertTrue(exported.contains("test-gvc"), "export should carry the configured values, got:\n" + exported);
    }
}
