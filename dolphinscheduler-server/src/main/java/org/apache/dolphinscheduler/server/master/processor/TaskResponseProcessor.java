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

package org.apache.dolphinscheduler.server.master.processor;

import io.netty.channel.Channel;
import org.apache.dolphinscheduler.common.enums.ExecutionStatus;
import org.apache.dolphinscheduler.common.thread.Stopper;
import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.common.utils.Preconditions;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.remote.command.Command;
import org.apache.dolphinscheduler.remote.command.CommandType;
import org.apache.dolphinscheduler.remote.command.TaskExecuteResponseCommand;
import org.apache.dolphinscheduler.remote.command.TaskResponseCommand;
import org.apache.dolphinscheduler.remote.processor.NettyRequestProcessor;
import org.apache.dolphinscheduler.remote.utils.FastJsonSerializer;
import org.apache.dolphinscheduler.server.master.cache.TaskInstanceCacheManager;
import org.apache.dolphinscheduler.server.master.cache.impl.TaskInstanceCacheManagerImpl;
import org.apache.dolphinscheduler.server.master.processor.queue.TaskResponseEvent;
import org.apache.dolphinscheduler.server.master.processor.queue.TaskResponseService;
import org.apache.dolphinscheduler.service.bean.SpringApplicationContext;
import org.apache.dolphinscheduler.service.process.ProcessService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.dolphinscheduler.common.Constants.SLEEP_TIME_MILLIS;

/**
 *  task response processor
 */
public class TaskResponseProcessor implements NettyRequestProcessor {

    private final Logger logger = LoggerFactory.getLogger(TaskResponseProcessor.class);

    /**
     * process service
     */
    private final TaskResponseService taskResponseService;

    /**
     * taskInstance cache manager
     */
    private final TaskInstanceCacheManager taskInstanceCacheManager;

    /**
     * processService
     */
    private ProcessService processService;

    public TaskResponseProcessor(){
        this.taskResponseService = SpringApplicationContext.getBean(TaskResponseService.class);
        this.taskInstanceCacheManager = SpringApplicationContext.getBean(TaskInstanceCacheManagerImpl.class);
        this.processService = SpringApplicationContext.getBean(ProcessService.class);
    }

    /**
     * task final result response
     * need master process , state persistence
     *
     * @param channel channel
     * @param command command
     */
    @Override
    public void process(Channel channel, Command command) {
        Preconditions.checkArgument(CommandType.TASK_EXECUTE_RESPONSE == command.getType(), String.format("invalid command type : %s", command.getType()));

        try {
            TaskExecuteResponseCommand responseCommand = FastJsonSerializer.deserialize(command.getBody(), TaskExecuteResponseCommand.class);
            logger.info("received command : {}", responseCommand);

            taskInstanceCacheManager.cacheTaskInstance(responseCommand);

            ExecutionStatus responseStatus = ExecutionStatus.of(responseCommand.getStatus());

            // TaskResponseEvent
            TaskResponseEvent taskResponseEvent = TaskResponseEvent.newResult(ExecutionStatus.of(responseCommand.getStatus()),
                    responseCommand.getEndTime(),
                    responseCommand.getProcessId(),
                    responseCommand.getAppIds(),
                    responseCommand.getTaskInstanceId());

            taskResponseService.addResponse(taskResponseEvent);

            while (Stopper.isRunning()){
                TaskInstance taskInstance = processService.findTaskInstanceById(taskResponseEvent.getTaskInstanceId());

                if (taskInstance != null && responseStatus.typeIsFinished()){
                    TaskResponseCommand taskResponseCommand = new TaskResponseCommand(ExecutionStatus.SUCCESS.getCode());
                    channel.writeAndFlush(taskResponseCommand.convert2Command());
                    break;
                }
                ThreadUtils.sleep(SLEEP_TIME_MILLIS);
            }
        }catch (Exception e){
            logger.error("task response process error : {}",e);
            TaskResponseCommand taskResponseCommand = new TaskResponseCommand(ExecutionStatus.FAILURE.getCode());
            channel.writeAndFlush(taskResponseCommand.convert2Command());
        }

    }


}
