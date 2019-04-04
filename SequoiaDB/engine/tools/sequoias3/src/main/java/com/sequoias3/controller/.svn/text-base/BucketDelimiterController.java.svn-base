package com.sequoias3.controller;

import com.sequoias3.common.RestParamDefine;
import com.sequoias3.core.User;
import com.sequoias3.exception.S3ServerException;
import com.sequoias3.model.DelimiterConfiguration;
import com.sequoias3.service.BucketDelimiterService;
import com.sequoias3.utils.RestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(RestParamDefine.REST_S3)
public class BucketDelimiterController {
    private static final Logger logger = LoggerFactory.getLogger(BucketController.class);

    @Autowired
    RestUtils restUtils;

    @Autowired
    BucketDelimiterService bucketDelimiterService;

    @PutMapping(value = "/{bucketname:.+}", params = RestParamDefine.DELIMITER,
            produces = MediaType.APPLICATION_XML_VALUE)
    public void putBucketDelimiter(@PathVariable("bucketname") String bucketName,
                                     @RequestHeader(RestParamDefine.AUTHORIZATION) String authorization,
                                     @RequestBody DelimiterConfiguration delimiterCon)
            throws S3ServerException {
        User operator = restUtils.getOperatorByAuthorization(authorization);

        logger.info("bucket=" + bucketName + "@delimiter" + delimiterCon.getDelimiter());

        bucketDelimiterService.putBucketDelimiter(operator.getUserId(), bucketName, delimiterCon.getDelimiter());
    }

    @GetMapping(value = "/{bucketname:.+}", params = RestParamDefine.DELIMITER,
            produces = MediaType.APPLICATION_XML_VALUE)
    public ResponseEntity getBucketDelimiter(@PathVariable("bucketname") String bucketName,
                                             @RequestHeader(RestParamDefine.AUTHORIZATION) String authorization)
            throws S3ServerException{
        User operator = restUtils.getOperatorByAuthorization(authorization);

        return ResponseEntity.ok(bucketDelimiterService.getBucketDelimiter(operator.getUserId(), bucketName));
    }
}
