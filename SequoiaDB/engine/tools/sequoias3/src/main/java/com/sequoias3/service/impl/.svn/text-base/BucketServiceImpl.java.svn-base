package com.sequoias3.service.impl;

import com.sequoias3.common.DBParamDefine;
import com.sequoias3.common.DelimiterStatus;
import com.sequoias3.common.VersioningStatusType;
import com.sequoias3.config.BucketConfig;
import com.sequoias3.core.*;
import com.sequoias3.dao.*;
import com.sequoias3.exception.S3Error;
import com.sequoias3.exception.S3ServerException;
import com.sequoias3.model.GetServiceResult;
import com.sequoias3.model.LocationConstraint;
import com.sequoias3.service.BucketService;
import com.sequoias3.service.ObjectService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class BucketServiceImpl implements BucketService {
    private static final Logger logger = LoggerFactory.getLogger(BucketServiceImpl.class);

    @Autowired
    BucketDao bucketDao;

    @Autowired
    UserDao userDao;

    @Autowired
    BucketConfig bucketConfig;

    @Autowired
    ObjectService objectService;

    @Autowired
    RegionDao regionDao;

    @Autowired
    DaoMgr daoMgr;

    @Autowired
    Transaction transaction;

    @Autowired
    IDGeneratorDao idGeneratorDao;

    @Override
    public void createBucket(long ownerID, String bucketName, String region) throws S3ServerException {
        int tryTime = DBParamDefine.DB_DUPLICATE_MAX_TIME;

        //check bucketname
        if (!isValidBucketName(bucketName)){
            throw new S3ServerException(S3Error.BUCKET_INVALID_BUCKETNAME,
                    "Invalid bucket name. bucket name = "+bucketName);
        }
        String newBucketName = bucketName.toLowerCase();
        while (tryTime > 0){
            tryTime--;
            ConnectionDao connection = daoMgr.getConnectionDao();
            transaction.begin(connection);
            try {
                //check duplicate bucket
                Bucket result = bucketDao.getBucketByName(newBucketName);
                if (null != result){
                    if (result.getOwnerId() == ownerID){
                        throw new S3ServerException(S3Error.BUCKET_ALREADY_OWNED_BY_YOU,
                                "Bucket already owned you. bucket name="+bucketName);
                    }else {
                        throw new S3ServerException(S3Error.BUCKET_ALREADY_EXIST,
                                "Bucket already exist. bucket name="+bucketName);
                    }
                }

                if (region != null) {
                    Region regionCon = regionDao.queryForUpdateRegion(connection, region);
                    if (null == regionCon){
                        throw new S3ServerException(S3Error.REGION_NO_SUCH_REGION,
                                "no such region. regionName:"+region);
                    }
                }

                //check bucket number
                long bucketLimit = bucketConfig.getLimit();
                long bucketCount = bucketDao.getBucketNumber(ownerID);
                if (bucketCount >= bucketLimit){
                    throw new S3ServerException(S3Error.BUCKET_TOO_MANY_BUCKETS,
                            "You have attempted to create more buckets than allowed. bucket count="
                                    +bucketCount+", bucket limit="+bucketLimit);
                }

                //insert bucket
                Bucket bucket = new Bucket();
                bucket.setBucketId(idGeneratorDao.getNewId(IDGenerator.TYPE_BUCKET));
                bucket.setBucketName(newBucketName);
                bucket.setOwnerId(ownerID);
                bucket.setTimeMillis(System.currentTimeMillis());
                bucket.setVersioningStatus(VersioningStatusType.NONE.getName());
                bucket.setDelimiter(1);
                bucket.setDelimiter1(DBParamDefine.DB_AUTO_DELIMITER);
                bucket.setDelimiter1CreateTime(System.currentTimeMillis());
                bucket.setDelimiter1ModTime(System.currentTimeMillis());
                bucket.setDelimiter1Status(DelimiterStatus.NORMAL.getName());
                bucket.setRegion(region);
                bucketDao.insertBucket(bucket);
                return;
            }catch (S3ServerException e) {
                logger.warn("Create bucket failed. bucket name = {}, error = {}", bucketName, e.getError().getErrIndex());
                if (e.getError().getErrIndex() == S3Error.DAO_DUPLICATE_KEY.getErrIndex() && tryTime > 0) {
                    continue;
                } else {
                    throw e;
                }
            }catch (Exception e){
                throw new S3ServerException(S3Error.BUCKET_CREATE_FAILED,
                        "create bucket failed. bucket name="+bucketName, e);
            }finally {
                transaction.rollback(connection);
                daoMgr.releaseConnectionDao(connection);
            }
        }
    }

    @Override
    public void deleteBucket(long ownerID, String bucketName) throws S3ServerException {
        try {
            String deleteName = bucketName.toLowerCase();

            //get and check bucket
            Bucket bucket = getBucket(ownerID, bucketName);

            //is bucket empty
            if (!isBucketEmpty(bucket)){
                throw new S3ServerException(S3Error.BUCKET_NOT_EMPTY,
                        "The bucket you tried to delete is not empty. bucket name = "+bucketName);
            }

            //delete bucket
            bucketDao.deleteBucket(deleteName);
        }catch (S3ServerException e) {
            throw e;
        }catch (Exception e){
            throw new S3ServerException(S3Error.BUCKET_DELETE_FAILED,
                    "delete bucket error. bucket name = "+bucketName);
        }
    }

    @Override
    public GetServiceResult getService(User owner) throws S3ServerException {
        GetServiceResult result = new GetServiceResult();
        try {
            //get owner
            result.setOwner(owner);

            //get bucket list
            List<Bucket> bucketArrayList = bucketDao.getBucketListByOwnerID(owner.getUserId());
            if (bucketArrayList.size() > 0){
                result.setBuckets(bucketArrayList);
            }

            return result;
        }catch (S3ServerException e) {
            throw e;
        }catch (Exception e){
            throw new S3ServerException(S3Error.BUCKET_GET_SERVICE_FAILED,
                    "Get bucket list error. ownerID="+owner.getUserId());
        }
    }

    @Override
    public Bucket getBucket(long ownerID, String bucketName)
            throws S3ServerException{
        Bucket bucket = bucketDao.getBucketByName(bucketName.toLowerCase());
        if (bucket == null){
            throw new S3ServerException(S3Error.BUCKET_NOT_EXIST,
                    "The specified bucket does not exist. bucket name = "+bucketName);
        }
        if (bucket.getOwnerId() != ownerID){
            throw new S3ServerException(S3Error.ACCESS_DENIED,
                    "You are not owned the specified bucket. bucket name = "+bucketName+", ownerID = "+ownerID);
        }

        return bucket;
    }

    @Override
    public void deleteBucketForce(Bucket bucket) throws S3ServerException {
        try {
            while (!isBucketEmpty(bucket)) {
                //delete objects in the bucket
                objectService.deleteObjectByBucket(bucket);
            }

            //delete bucket
            bucketDao.deleteBucket(bucket.getBucketName());
        }catch (S3ServerException e) {
            throw e;
        }catch (Exception e){
            throw new S3ServerException(S3Error.BUCKET_DELETE_FAILED,
                    "delete bucket error. bucket name = "+bucket.getBucketName());
        }
    }

    @Override
    public LocationConstraint getBucketLocation(long ownerID, String bucketName) throws S3ServerException {
        try{
            Bucket bucket = getBucket(ownerID, bucketName);
            LocationConstraint locationConstraint = new LocationConstraint();
            locationConstraint.setLocation(bucket.getRegion());
            return locationConstraint;
        }catch (S3ServerException e){
            throw e;
        }catch (Exception e){
            throw new S3ServerException(S3Error.BUCKET_LOCATION_GET_FAILED,
                    "get bucket location failed. bucketName="+bucketName, e);
        }
    }

    private Boolean isValidBucketName(String bucketName){
        if (bucketName.length() < 3 || bucketName.length() > 63){
            return false;
        }
        return true;
    }

    public Boolean isBucketEmpty(Bucket bucket)throws S3ServerException {
        if (objectService.getObjectNumberByBucketId(bucket) > 0){
            return false;
        }
        return true;
    }
}
