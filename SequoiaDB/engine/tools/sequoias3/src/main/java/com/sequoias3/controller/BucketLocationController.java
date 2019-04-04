package com.sequoias3.controller;

import com.sequoias3.common.RestParamDefine;
import com.sequoias3.core.User;
import com.sequoias3.exception.S3ServerException;
import com.sequoias3.service.BucketService;
import com.sequoias3.utils.RestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(RestParamDefine.REST_S3)
public class BucketLocationController {
    private static final Logger logger = LoggerFactory.getLogger(BucketLocationController.class);

    @Autowired
    RestUtils restUtils;

    @Autowired
    BucketService bucketService;

    @GetMapping(value = "/{bucketname:.+}", params = RestParamDefine.LOCATION, produces = MediaType.APPLICATION_XML_VALUE)
    public ResponseEntity getBucketLocation(@PathVariable("bucketname") String bucketName,
                                            @RequestHeader(RestParamDefine.AUTHORIZATION) String authorization)
            throws S3ServerException {
        User operator = restUtils.getOperatorByAuthorization(authorization);

        logger.info("get bucket location. operator={}", operator.getUserName());

        return ResponseEntity.ok()
                .body(bucketService.getBucketLocation(operator.getUserId(), bucketName));
    }
}
