package com.sequoias3.core;

import com.sequoias3.common.RestParamDefine;
import com.sequoias3.exception.S3Error;
import com.sequoias3.exception.S3ServerException;

public class Range {
    private long start;
    private long end;
    private long contentLength;

    public Range(String range) throws S3ServerException {
        int beginIndex = range.indexOf(RestParamDefine.REST_RANGE_START);
        if (-1 == beginIndex){
            throw new S3ServerException(S3Error.OBJECT_INVALID_RANGE, "range is invalid. range:"+range);
        }
        String substring = range.substring(beginIndex+RestParamDefine.REST_RANGE_START.length(), range.length());

        String[] numbers = substring.split(RestParamDefine.REST_HYPHEN);
        if (numbers.length < 2){
            throw new S3ServerException(S3Error.OBJECT_INVALID_RANGE, "range  is invalid"+range);
        }
        try {
            if (numbers[0].length() > 0){
                this.start = Long.parseLong(numbers[0]) ;
            }else {
                this.start = -1;
            }
            if (numbers[1].length() > 0){
                this.end = Long.parseLong(numbers[1]);
            }else {
                this.end = -1;
            }
            if (-1 == this.start && -1 == this.end){
                throw new S3ServerException(S3Error.OBJECT_INVALID_RANGE, "range is invalid"+range);
            }
            if (this.start > this.end){
                throw new S3ServerException(S3Error.OBJECT_INVALID_RANGE, "range is invalid"+range);
            }
        }catch (NumberFormatException e){
            throw new S3ServerException(S3Error.OBJECT_INVALID_RANGE, "range is invalid"+range);
        }catch (S3ServerException e){
            throw e;
        }
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getStart() {
        return start;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public long getEnd() {
        return end;
    }

    public void setContentLength(long contentLength) {
        this.contentLength = contentLength;
    }

    public long getContentLength() {
        return contentLength;
    }
}
