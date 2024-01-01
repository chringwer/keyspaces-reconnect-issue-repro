package com.example.demo;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultTopologyMonitor;
import com.datastax.oss.driver.internal.core.metadata.NodeInfo;
import com.datastax.oss.driver.shaded.guava.common.collect.Streams;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

public class InterceptingTopologyMonitor extends DefaultTopologyMonitor {
  private final List<Map<UUID, EndPoint>> history;

  public InterceptingTopologyMonitor(DefaultDriverContext defaultDriverContext) {
    super(defaultDriverContext);

    history = new LinkedList<>();
  }

  public List<Map<UUID, EndPoint>> getHistory() {
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

              history.add(
                  Streams.stream(nodeInfos)
                      .collect(
                          Collectors.toMap(
                              NodeInfo::getHostId,
                              NodeInfo::getEndPoint,
                              (l, r) -> l,
                              TreeMap::new)));
            });
  }
}
