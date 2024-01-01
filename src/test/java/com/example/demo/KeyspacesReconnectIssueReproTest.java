package com.example.demo;

import static java.net.InetSocketAddress.createUnresolved;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.control.ControlConnection;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metadata.NodeStateEvent;
import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import com.datastax.oss.driver.shaded.guava.common.collect.Multimap;
import com.datastax.oss.driver.shaded.guava.common.collect.MultimapBuilder;
import com.datastax.oss.driver.shaded.guava.common.collect.Multimaps;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import software.aws.mcs.auth.SigV4AuthProvider;

public class KeyspacesReconnectIssueReproTest {
  private static final String AWS_KEYSPACES_ENDPOINT = "cassandra.eu-west-1.amazonaws.com";
  private static final String AWS_KEYSPACES_REGION = "eu-west-1";

  @Test
  public void shouldReturnStableKeysOnNodeListRefresh() throws InterruptedException {
    CqlSession cqlSession =
        new InterceptingCqlSessionBuilder()
            .withConfigLoader(DriverConfigLoader.fromClasspath("keyspaces.conf"))
            .addContactPoint(createUnresolved(AWS_KEYSPACES_ENDPOINT, 9142))
            .withLocalDatacenter(AWS_KEYSPACES_REGION)
            .withAuthProvider(new SigV4AuthProvider(AWS_KEYSPACES_REGION))
            .build();

    DefaultDriverContext context = (DefaultDriverContext) cqlSession.getContext();
    ControlConnection controlConnection = context.getControlConnection();

    Node node =
        context
            .getMetadataManager()
            .getMetadata()
            .findNode(controlConnection.channel().getEndPoint())
            .orElseThrow();

    context.getEventBus().fire(NodeStateEvent.removed((DefaultNode) node));

    Thread.sleep(TimeUnit.SECONDS.toMillis(5));

    cqlSession.close();

    List<Map<UUID, EndPoint>> nodeListHistory =
        ((InterceptingTopologyMonitor) context.getTopologyMonitor()).getHistory();

    SoftAssertions assertions = new SoftAssertions();

    assertions
        .assertThat(findAmbiguousEndpoints(nodeListHistory))
        .as("Ambiguous Endpoints By HostId")
        .isEmpty();

    assertions
        .assertThat(findAmbiguousHostIds(nodeListHistory))
        .as("Ambiguous HostIds By Endpoint")
        .isEmpty();

    assertions.assertAll();
  }

  private static Map<EndPoint, Collection<UUID>> findAmbiguousEndpoints(
      List<Map<UUID, EndPoint>> nodeListHistory) {
    Multimap<EndPoint, UUID> hostIdByEndpoint =
        nodeListHistory.stream()
            .flatMap(nodeList -> nodeList.entrySet().stream())
            .collect(
                Multimaps.toMultimap(
                    Entry::getValue,
                    Entry::getKey,
                    MultimapBuilder.hashKeys().treeSetValues()::build));

    return Maps.filterValues(hostIdByEndpoint.asMap(), values -> values.size() > 1);
  }

  private static Map<UUID, Collection<EndPoint>> findAmbiguousHostIds(
      List<Map<UUID, EndPoint>> nodeListHistory) {
    Multimap<UUID, EndPoint> endpointsByHostId =
        nodeListHistory.stream()
            .flatMap(nodeList -> nodeList.entrySet().stream())
            .collect(
                Multimaps.toMultimap(
                    Entry::getKey,
                    Entry::getValue,
                    MultimapBuilder.treeKeys().hashSetValues()::build));

    return Maps.filterValues(endpointsByHostId.asMap(), values -> values.size() > 1);
  }
}
