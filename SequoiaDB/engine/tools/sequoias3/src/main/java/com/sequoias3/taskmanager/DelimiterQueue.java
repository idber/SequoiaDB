package com.sequoias3.taskmanager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Component("DelimiterQueue")
public class DelimiterQueue {
    private static final Logger logger = LoggerFactory.getLogger(DelimiterQueue.class);
    private Lock taskLock = new ReentrantLock();

    private List<String> bucketList = new LinkedList<>();

    public void addBucketName(String bucketName){
        taskLock.lock();
        try{
            bucketList.add(bucketName);
        }finally {
            taskLock.unlock();
        }
    }

    public String getBucketName(){
        taskLock.lock();
        try {
            if (bucketList.size() > 0) {
                String bucketName = bucketList.get(0);
                bucketList.remove(0);
                return bucketName;
            } else {
                return null;
            }
        }finally {
            taskLock.unlock();
        }
    }
}
