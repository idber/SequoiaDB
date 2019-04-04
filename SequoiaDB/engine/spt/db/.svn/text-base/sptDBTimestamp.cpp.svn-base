/*******************************************************************************

   Copyright (C) 2011-2018 SequoiaDB Ltd.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU Affero General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.

   Source File Name = sptDBTimestamp.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          19/01/2018  WJM  Initial Draft

   Last Changed =

*******************************************************************************/
#include "sptDBTimestamp.hpp"
#include "utilStr.hpp"
using namespace bson ;
namespace engine
{
   #define SPT_TIMESTAMP_NAME "Timestamp"
   #define SPT_TIMESTAMP_TIME_FIELD "_t"
   #define SPT_TIMESTAMP_SPECIAL_FIELD "$timestamp"
   JS_CONSTRUCT_FUNC_DEFINE( _sptDBTimestamp, construct )
   JS_DESTRUCT_FUNC_DEFINE( _sptDBTimestamp, destruct )

   JS_BEGIN_MAPPING( _sptDBTimestamp, SPT_TIMESTAMP_NAME )
      JS_ADD_CONSTRUCT_FUNC( construct )
      JS_ADD_DESTRUCT_FUNC( destruct )
      JS_SET_SPECIAL_FIELD_NAME( SPT_TIMESTAMP_SPECIAL_FIELD )
      JS_SET_CVT_TO_BSON_FUNC( _sptDBTimestamp::cvtToBSON )
      JS_SET_JSOBJ_TO_BSON_FUNC( _sptDBTimestamp::fmpToBSON )
      JS_SET_BSON_TO_JSOBJ_FUNC( _sptDBTimestamp::bsonToJSObj )
   JS_MAPPING_END()

   _sptDBTimestamp::_sptDBTimestamp()
   {
   }

   _sptDBTimestamp::~_sptDBTimestamp()
   {
   }

   INT32 _sptDBTimestamp::construct( const _sptArguments &arg,
                                     _sptReturnVal &rval,
                                     bson::BSONObj &detail )
   {
      INT32 rc = SDB_OK ;
      string timeStr ;
      if( arg.argc() == 0 )
      {
         ossTimestamp currentTime ;
         struct tm localTm ;
         time_t t ;
         CHAR buf[128] ;
         ossGetCurrentTime( currentTime ) ;
         t = currentTime.time ;
         ossLocalTime( t, localTm ) ;
         ossSnprintf( buf, 127,
                      "%04d-%02d-%02d-%02d.%02d.%02d.%06d",
                      localTm.tm_year+1900,            // 1) Year (UINT32)
                      localTm.tm_mon+1,                // 2) Month (UINT32)
                      localTm.tm_mday,                 // 3) Day (UINT32)
                      localTm.tm_hour,                 // 4) Hour (UINT32)
                      localTm.tm_min,                  // 5) Minute (UINT32)
                      localTm.tm_sec,                  // 6) Second (UINT32)
                      currentTime.microtm ) ;
         timeStr = buf ;
      }
      else if( arg.argc() == 1 )
      {
         time_t tm ;
         UINT64 usec = 0 ;

         rc = arg.getString( 0, timeStr ) ;
         if( SDB_OK != rc )
         {
            detail = BSON( SPT_ERR << "Time must be string" ) ;
            goto error ;
         }
         rc = utilStr2TimeT( timeStr.c_str(), tm, &usec ) ;
         if( SDB_OK != rc )
         {
            detail = BSON( SPT_ERR << "Failed to convert string to time" );
            goto error ;
         }
      }
      else if( arg.argc() == 2 )
      {
         struct tm localTm ;
         time_t t ;
         INT32 time = 0 ;
         INT32 inc = 0 ;
         CHAR buf[128] ;
         if( !arg.isInt( 0 ) || !arg.isInt( 1 ) )
         {
            rc = SDB_INVALIDARG ;
            detail = BSON( SPT_ERR << "Argument must be int" ) ;
            goto error ;
         }
         rc = arg.getNative( 0, &time, SPT_NATIVE_INT32 ) ;
         if( SDB_OK != rc )
         {
            detail = BSON( SPT_ERR << "Second must be int" ) ;
            goto error ;
         }
         rc = arg.getNative( 1, &inc, SPT_NATIVE_INT32 ) ;
         if( SDB_OK != rc )
         {
            detail = BSON( SPT_ERR << "Microsecond must be int" ) ;
            goto error ;
         }
         t = time ;
         if( inc < 0 )
         {
            t -= 1 ; // sec
            inc += 1000000 ; // us
         }
         ossLocalTime( t, localTm ) ;
         ossSnprintf( buf, 127,
                      "%04d-%02d-%02d-%02d.%02d.%02d.%06d",
                      localTm.tm_year+1900,            // 1) Year (UINT32)
                      localTm.tm_mon+1,                // 2) Month (UINT32)
                      localTm.tm_mday,                 // 3) Day (UINT32)
                      localTm.tm_hour,                 // 4) Hour (UINT32)
                      localTm.tm_min,                  // 5) Minute (UINT32)
                      localTm.tm_sec,                  // 6) Second (UINT32)
                      inc ) ;
         timeStr = buf ;
      }
      else
      {
         rc = SDB_INVALIDARG ;
         detail = BSON( SPT_ERR << "Inalid argument number" ) ;
         goto error ;
      }
      rval.addSelfProperty( SPT_TIMESTAMP_TIME_FIELD )->setValue( timeStr ) ;
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptDBTimestamp::destruct()
   {
      return SDB_OK ;
   }
   INT32 _sptDBTimestamp::cvtToBSON( const CHAR* key, const sptObject &value,
                                     BOOLEAN isSpecialObj,
                                     BSONObjBuilder& builder,
                                     string &errMsg )
   {
      INT32 rc = SDB_OK ;
      string timeStr ;
      time_t tm ;
      UINT64 usec = 0 ;
      if( isSpecialObj )
      {
         rc = value.getStringField( SPT_TIMESTAMP_SPECIAL_FIELD, timeStr ) ;
      }
      else
      {
         rc = value.getStringField( SPT_TIMESTAMP_TIME_FIELD, timeStr ) ;
      }
      if( SDB_OK != rc )
      {
         errMsg = "Failed to get Timestamp property" ;
         goto error ;
      }
      rc = engine::utilStr2TimeT( timeStr.c_str(), tm, &usec ) ;
      if( SDB_OK != rc )
      {
         errMsg = "Failed to convert Timestamp property" ;
      }
      if ( !ossIsTimestampValid( ( INT64 )tm ) )
      {
         rc = SDB_INVALIDARG ;
         errMsg = "Invalid timestamp value" ;
         goto error ;
      }
      builder.appendTimestamp( key, tm * 1000, usec ) ;
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptDBTimestamp::fmpToBSON( const sptObject &value, BSONObj &retObj,
                                     string &errMsg )
   {
      INT32 rc = SDB_OK ;
      string data ;
      rc = value.getStringField( SPT_TIMESTAMP_TIME_FIELD, data ) ;
      if( SDB_OK != rc )
      {
         errMsg = "Failed to get _t field" ;
         goto error ;
      }
      retObj = BSON( SPT_TIMESTAMP_TIME_FIELD << data ) ;
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptDBTimestamp::bsonToJSObj( sdbclient::sdb &db, const BSONObj &data,
                                       _sptReturnVal &rval,
                                       bson::BSONObj &detail )
   {
      INT32 rc = SDB_OK ;
      string timeStr ;
      timeStr = data.getStringField( SPT_TIMESTAMP_TIME_FIELD ) ;
      sptDBTimestamp *pTimestamp = SDB_OSS_NEW sptDBTimestamp() ;
      if( NULL == pTimestamp )
      {
         rc = SDB_OOM ;
         detail = BSON( SPT_ERR << "Failed to new sptDBTimestamp obj" ) ;
         goto error ;
      }
      rc = rval.setUsrObjectVal< sptDBTimestamp >( pTimestamp ) ;
      if( SDB_OK != rc )
      {
         detail = BSON( SPT_ERR << "Failed to set return obj" ) ;
         goto error ;
      }
      rval.addReturnValProperty( SPT_TIMESTAMP_TIME_FIELD )->setValue( timeStr ) ;
   done:
      return rc ;
   error:
      SAFE_OSS_DELETE( pTimestamp ) ;
      goto done ;
   }
}
