package com.sequoias3.dao;

import com.sequoias3.core.DataAttr;
import com.sequoias3.core.Region;
import com.sequoias3.exception.S3ServerException;
import org.bson.types.ObjectId;

import java.io.InputStream;
import java.util.Date;

public interface DataDao {
    DataAttr insertObjectData(String csName, String clName, InputStream data, Region region) throws S3ServerException;

    DataLob getDataLobForRead(String csName, String clName, ObjectId lobId) throws S3ServerException;

    void releaseDataLob(DataLob dataLob);

    void deleteObjectDataByLobId(ConnectionDao connectionDao, String csName, String clName, ObjectId lobId) throws S3ServerException;


}
