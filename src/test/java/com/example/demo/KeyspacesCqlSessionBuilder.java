package com.example.demo;

import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultTopologyMonitor;
import com.datastax.oss.driver.internal.core.metadata.NodeInfo;
import com.datastax.oss.driver.internal.core.metadata.TopologyMonitor;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

public class KeyspacesCqlSessionBuilder extends CqlSessionBuilder {
  @Override
  protected DriverContext buildContext(
      DriverConfigLoader configLoader, ProgrammaticArguments programmaticArguments) {
    return new DefaultDriverContext(configLoader, programmaticArguments) {
      @Override
      protected TopologyMonitor buildTopologyMonitor() {
        return new KeyspacesTopologyMonitor(this);
      }
    };
  }

   static class KeyspacesTopologyMonitor extends DefaultTopologyMonitor {
    private final Map<EndPoint, UUID> hostIdsByEndpoint;

    public KeyspacesTopologyMonitor(InternalDriverContext context) {
      super(context);

      hostIdsByEndpoint = new ConcurrentHashMap<>();
    }

    @Override
    public CompletionStage<Iterable<NodeInfo>> refreshNodeList() {
      return super.refreshNodeList()
          .thenApply(
              nodeInfos -> {
                List<NodeInfo> result = new LinkedList<>();

                for (NodeInfo nodeInfo : nodeInfos) {
                  UUID nextHostId = nodeInfo.getHostId();
                  UUID previousHostId = hostIdsByEndpoint.put(nodeInfo.getEndPoint(), nextHostId);

                  if (previousHostId != null && !previousHostId.equals(nextHostId)) {
                    result.add(new NodeInfoAlias(nodeInfo, previousHostId));
                  } else {
                    result.add(nodeInfo);
                  }
                }

                return result;
              });
    }
  }

  private static class NodeInfoAlias implements NodeInfo {
    private final UUID hostId;
    private final NodeInfo delegate;

    public NodeInfoAlias(NodeInfo delegate, UUID hostId) {
      this.delegate = delegate;
      this.hostId = hostId;
    }

    @Override
    public Optional<InetSocketAddress> getBroadcastAddress() {
      return delegate.getBroadcastAddress();
    }

    @Override
    public Optional<InetSocketAddress> getBroadcastRpcAddress() {
      return delegate.getBroadcastRpcAddress();
    }

    @Override
    public String getCassandraVersion() {
      return delegate.getCassandraVersion();
    }

    @Override
    public String getDatacenter() {
      return delegate.getDatacenter();
    }

    @Override
    public EndPoint getEndPoint() {
      return delegate.getEndPoint();
    }

    @Override
    public Map<String, Object> getExtras() {
      return delegate.getExtras();
    }

    @Override
    public UUID getHostId() {
      return hostId;
    }

    @Override
    public Optional<InetSocketAddress> getListenAddress() {
      return delegate.getListenAddress();
    }

    @Override
    public String getPartitioner() {
      return delegate.getPartitioner();
    }

    @Override
    public String getRack() {
      return delegate.getRack();
    }

    @Override
    public UUID getSchemaVersion() {
      return delegate.getSchemaVersion();
    }

    @Override
    public Set<String> getTokens() {
      return delegate.getTokens();
    }
  }
}
