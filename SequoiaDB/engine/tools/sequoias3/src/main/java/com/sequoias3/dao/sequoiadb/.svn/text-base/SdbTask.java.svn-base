package com.sequoias3.dao.sequoiadb;

import com.sequoiadb.base.CollectionSpace;
import com.sequoiadb.base.DBCollection;
import com.sequoiadb.base.DBQuery;
import com.sequoiadb.base.Sequoiadb;
import com.sequoias3.config.SequoiadbConfig;
import com.sequoias3.core.TaskTable;
import com.sequoias3.dao.ConnectionDao;
import com.sequoias3.dao.DaoCollectionDefine;
import com.sequoias3.dao.TaskDao;
import com.sequoias3.exception.S3ServerException;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

@Repository("TaskDao")
public class SdbTask implements TaskDao {

    @Autowired
    SdbDataSourceWrapper sdbDatasourceWrapper;

    @Autowired
    SequoiadbConfig config;

    @Override
    public void insertTaskId(ConnectionDao connection, Long taskId)
            throws S3ServerException {
        try {
            Sequoiadb sdb      = ((SdbConnectionDao)connection).getConnection();
            CollectionSpace cs = sdb.getCollectionSpace(config.getMetaCsName());
            DBCollection cl    =  cs.getCollection(DaoCollectionDefine.TASK_COLLECTION);

            BSONObject insert = new BasicBSONObject();
            insert.put(TaskTable.TASK_ID, taskId);
            insert.put(TaskTable.TASK_TYPE, TaskTable.TASK_TYPE_DELIMITER);

            cl.insert(insert);
        }catch (Exception e){
            throw e;
        }
    }

    @Override
    public Long queryTaskId(ConnectionDao connection, Long taskId) throws S3ServerException {
        try {
            Sequoiadb sdb = ((SdbConnectionDao) connection).getConnection();
            CollectionSpace cs = sdb.getCollectionSpace(config.getMetaCsName());
            DBCollection cl = cs.getCollection(DaoCollectionDefine.TASK_COLLECTION);

            BSONObject matcher = new BasicBSONObject();
            matcher.put(TaskTable.TASK_TYPE, TaskTable.TASK_TYPE_DELIMITER);
            matcher.put(TaskTable.TASK_ID, taskId);

            BSONObject result = cl.queryOne(matcher, null, null, null, DBQuery.FLG_QUERY_FOR_UPDATE);
            Long id = null;
            if (result != null) {
                id = (long) result.get(TaskTable.TASK_ID);
            }
            return id;
        }catch (Exception e){
            throw e;
        }
    }

    @Override
    public void deleteTaskId(ConnectionDao connection, Long taskId) throws S3ServerException {
        try {
            Sequoiadb sdb = ((SdbConnectionDao) connection).getConnection();
            CollectionSpace cs = sdb.getCollectionSpace(config.getMetaCsName());
            DBCollection cl = cs.getCollection(DaoCollectionDefine.TASK_COLLECTION);

            BSONObject matcher = new BasicBSONObject();
            matcher.put(TaskTable.TASK_TYPE, TaskTable.TASK_TYPE_DELIMITER);
            matcher.put(TaskTable.TASK_ID, taskId);

            cl.delete(matcher);
        }catch (Exception e){
            throw e;
        }
    }
}
