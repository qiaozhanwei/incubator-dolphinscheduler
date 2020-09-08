package org.apache.dolphinscheduler.server.worker.cache;

import org.apache.dolphinscheduler.remote.command.Command;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ResponceCache {

    private static volatile ResponceCache instance = null;

    public static ResponceCache get(){
        if (instance == null){
            synchronized (ResponceCache.class){
                if (instance == null){
                    instance = new ResponceCache();
                }
            }
        }
        return instance;
    }

    private Map<Integer,ArrayList<Command>> responseCache = new ConcurrentHashMap<>();

    public void cache(Integer taskInstanceId,Command command){
        ArrayList<Command> commands = responseCache.get(taskInstanceId);
        if (commands == null){
            commands = new ArrayList<>(2);
            responseCache.put(taskInstanceId,commands);
        }
        commands.add(command);
    }

    public ArrayList<Command> get(Integer taskInstanceId){
        return responseCache.get(taskInstanceId);
    }

    public void remove(Integer taskInstanceId){
        responseCache.remove(taskInstanceId);
    }
}
