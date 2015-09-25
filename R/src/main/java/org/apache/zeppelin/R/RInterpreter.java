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

package org.apache.zeppelin.R;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.REngineException;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * R interpreter for Zeppelin.
 *
 */
public class RInterpreter extends Interpreter {
  Logger logger = LoggerFactory.getLogger(RInterpreter.class);

  int commandTimeOut = 600000;

  HashMap<String, RConnection> connections = new HashMap<String, RConnection>();

  static {
    Interpreter.register("r", RInterpreter.class.getName());
  }

  public RInterpreter(Properties property) {
    super(property);
  }

  @Override
  public void open() {}

  @Override
  public void close() {}


  @Override
  public InterpreterResult interpret(String cmd, InterpreterContext contextInterpreter) {

    RConnection c = connections.get(contextInterpreter.getNoteId());

    if (c == null) {
      try {
        c = new RConnection();
        connections.put(contextInterpreter.getNoteId(), c);
      } catch (RserveException e) {
        e.printStackTrace();
      }
    }

    try {
      c.assign(".tmp.", cmd);
      REXP r = c.parseAndEval("try(eval(parse(text=.tmp.)),silent=TRUE)");
      if (r == null) {
        return new InterpreterResult(InterpreterResult.Code.SUCCESS, "OK");
      } else if (r.isRaw()) {
        System.out.println(r.asBytes());
      } else if (r.inherits("try-error")) {
        return new InterpreterResult(InterpreterResult.Code.ERROR, r.asString());
      } else { 
        return new InterpreterResult(InterpreterResult.Code.SUCCESS, r.asString());
      }

    } catch (RserveException | REXPMismatchException e) {
      e.printStackTrace();
    } catch (REngineException e) {
      e.printStackTrace();
    }
    return new InterpreterResult(InterpreterResult.Code.SUCCESS, "YEAH");
  }

  @Override
  public void cancel(InterpreterContext context) {}

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public Scheduler getScheduler() {
    return SchedulerFactory.singleton().createOrGetFIFOScheduler(
      RInterpreter.class.getName() + this.hashCode());
  }

  @Override
  public List<String> completion(String buf, int cursor) {
    return null;
  }

}
