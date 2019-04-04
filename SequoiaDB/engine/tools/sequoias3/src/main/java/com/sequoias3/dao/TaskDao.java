package com.sequoias3.dao;

import com.sequoias3.exception.S3ServerException;

public interface TaskDao {
    void insertTaskId(ConnectionDao connection, Long taskId) throws S3ServerException;

    Long queryTaskId(ConnectionDao connection, Long taskId) throws S3ServerException;

    void deleteTaskId(ConnectionDao connection, Long taskId) throws S3ServerException;
}
