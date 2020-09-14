package org.apache.dolphinscheduler.server.worker.runner;

import org.apache.dolphinscheduler.common.thread.Stopper;

import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.remote.command.Command;
import org.apache.dolphinscheduler.server.worker.cache.ResponceCache;
import org.apache.dolphinscheduler.server.worker.processor.TaskCallbackService;
import org.apache.dolphinscheduler.service.bean.SpringApplicationContext;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class RetryReportTaskStatusThread implements Runnable {

    private static long RETRY_REPORT_TASK_STATUS_TIME = 5 * 60 * 1000;
    /**
     *  task callback service
     */
    private final TaskCallbackService taskCallbackService;

    public void start(){
        Thread thread = new Thread(this,"RetryReportTaskStatusThread");
        thread.start();
    }

    public RetryReportTaskStatusThread(){
        this.taskCallbackService = SpringApplicationContext.getBean(TaskCallbackService.class);
    }
    @Override
    public void run() {
        ResponceCache responceCache = ResponceCache.get();

        while (Stopper.isRunning()){
            if (responceCache.getAckCache().isEmpty() && responceCache.getResponseCache().isEmpty()){
                continue;
            }

            if (!responceCache.getAckCache().isEmpty()){
                Map<Integer,Command> ackCache =  responceCache.getAckCache();
                for (Map.Entry<Integer, Command> entry : ackCache.entrySet()){
                    Integer taskInstanceId = entry.getKey();
                    Command ackCommand = entry.getValue();
                    taskCallbackService.sendAck(taskInstanceId,ackCommand);
                }
            }

            if (!responceCache.getResponseCache().isEmpty()){
                Map<Integer,Command> responseCache =  responceCache.getResponseCache();
                for (Map.Entry<Integer, Command> entry : responseCache.entrySet()){
                    Integer taskInstanceId = entry.getKey();
                    Command responseCommand = entry.getValue();
                    taskCallbackService.sendAck(taskInstanceId,responseCommand);
                }
            }

            ThreadUtils.sleep(RETRY_REPORT_TASK_STATUS_TIME);
        }
    }
}
