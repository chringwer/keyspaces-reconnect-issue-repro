package com.example.demo;

import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.metadata.TopologyMonitor;

public class InterceptingCqlSessionBuilder extends CqlSessionBuilder {
  @Override
  protected DriverContext buildContext(
      DriverConfigLoader configLoader, ProgrammaticArguments programmaticArguments) {
    return new DefaultDriverContext(configLoader, programmaticArguments) {
      @Override
      protected TopologyMonitor buildTopologyMonitor() {
        return new InterceptingTopologyMonitor(this);
      }
    };
  }
}
