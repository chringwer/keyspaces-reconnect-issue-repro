package com.example.demo;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultTopologyMonitor;
import com.datastax.oss.driver.internal.core.metadata.NodeInfo;
import com.datastax.oss.driver.shaded.guava.common.collect.Streams;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

public class InterceptingTopologyMonitor extends DefaultTopologyMonitor {
  private final List<IntermediateNodeList> history;
  private final DefaultDriverContext defaultDriverContext;

  public InterceptingTopologyMonitor(DefaultDriverContext defaultDriverContext) {
    super(defaultDriverContext);

    this.defaultDriverContext = defaultDriverContext;

    history = new LinkedList<>();
  }

  public List<IntermediateNodeList> getHistory() {
    return history;
  }

  @Override
  public CompletionStage<Iterable<NodeInfo>> refreshNodeList() {
    return super.refreshNodeList()
        .whenComplete(
            (nodeInfos, throwable) -> {
              if (throwable != null) {
                return;
              }

              EndPoint controlEndPoint =
                  defaultDriverContext.getControlConnection().channel().getEndPoint();

              TreeMap<UUID, EndPoint> endpointsById =
                  Streams.stream(nodeInfos)
                      .collect(
                          Collectors.toMap(
                              NodeInfo::getHostId,
                              NodeInfo::getEndPoint,
                              (l, r) -> l,
                              TreeMap::new));

              history.add(new IntermediateNodeList(controlEndPoint, endpointsById));
            });
  }

  public record IntermediateNodeList(
      EndPoint controlEndPoint, TreeMap<UUID, EndPoint> endpointsById) {
    public void print() {
      System.err.println("ControlEndpoint: " + controlEndPoint);
      System.err.println("..");

      endpointsById.forEach(
          (id, endpoint) -> {
            if (endpoint.equals(controlEndPoint)) {
              System.err.print(">> ");
            }

            System.err.println(id + " => " + endpoint);
          });
    }
  }
}
