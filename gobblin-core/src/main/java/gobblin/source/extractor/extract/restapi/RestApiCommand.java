/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gobblin.source.extractor.extract.restapi;

import gobblin.source.extractor.extract.Command;
import gobblin.source.extractor.extract.CommandType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.base.Joiner;


public class RestApiCommand implements Command {

  public enum RestApiCommandType implements CommandType {
    GET,
    PUT
  }

  private List<String> params;
  private RestApiCommandType cmd;

  public RestApiCommand() {
    this.params = new ArrayList<>();
  }

  @Override
  public List<String> getParams() {
    return this.params;
  }

  @Override
  public CommandType getCommandType() {
    return this.cmd;
  }

  @Override
  public Command build(Collection<String> params, CommandType cmd) {
    this.params.addAll(params);
    this.cmd = (RestApiCommandType) cmd;
    return this;
  }

  @Override
  public String toString() {
    Joiner joiner = Joiner.on(":").skipNulls();
    return this.cmd.toString() + ":" + joiner.join(this.params);
  }
}
