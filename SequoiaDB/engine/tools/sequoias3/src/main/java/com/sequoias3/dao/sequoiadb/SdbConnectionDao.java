package com.sequoias3.dao.sequoiadb;

import com.sequoiadb.base.Sequoiadb;
import com.sequoias3.dao.ConnectionDao;
import com.sequoias3.exception.S3ServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SdbConnectionDao implements ConnectionDao {
    private static final Logger logger = LoggerFactory.getLogger(SdbConnectionDao.class);

    private Sequoiadb sdb;

    public SdbConnectionDao(SdbDataSourceWrapper sdbDatasourceWrapper) throws S3ServerException{
        sdb = sdbDatasourceWrapper.getSequoiadb();
    }

    public Sequoiadb getConnection() {
        return sdb;
    }

    public void setSdb(Sequoiadb sdb) {
        this.sdb = sdb;
    }
}
