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

   Source File Name = sptDBNumberDecimal.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          22/01/2018  WJM  Initial Draft

   Last Changed =

*******************************************************************************/
#include "sptDBNumberDecimal.hpp"
using namespace bson ;
namespace engine
{
   #define SPT_NUMBERDECIMAL_SPECIALOBJ_DECIMAL_FIELD   "$decimal"
   #define SPT_NUMBERDECIMAL_SPECIALOBJ_PRECISION_FIELD "$precision"
   JS_CONSTRUCT_FUNC_DEFINE( _sptDBNumberDecimal, construct )
   JS_DESTRUCT_FUNC_DEFINE( _sptDBNumberDecimal, destruct )

   JS_BEGIN_MAPPING( _sptDBNumberDecimal, "NumberDecimal" )
      JS_ADD_CONSTRUCT_FUNC( construct )
      JS_ADD_DESTRUCT_FUNC( destruct )
      JS_SET_SPECIAL_FIELD_NAME( SPT_NUMBERDECIMAL_SPECIALOBJ_DECIMAL_FIELD )
      JS_SET_CVT_TO_BSON_FUNC( _sptDBNumberDecimal::cvtToBSON )
      JS_SET_JSOBJ_TO_BSON_FUNC( _sptDBNumberDecimal::fmpToBSON )
      JS_SET_BSON_TO_JSOBJ_FUNC( _sptDBNumberDecimal::bsonToJSObj )
   JS_MAPPING_END()

   _sptDBNumberDecimal::_sptDBNumberDecimal()
   {
   }

   _sptDBNumberDecimal::~_sptDBNumberDecimal()
   {
   }

   INT32 _sptDBNumberDecimal::construct( const _sptArguments &arg,
                                  _sptReturnVal &rval,
                                  bson::BSONObj &detail )
   {
      detail = BSON( SPT_ERR << "use of new SdbReplicaGroup() is forbidden" ) ;
      return SDB_SYS ;
   }

   INT32 _sptDBNumberDecimal::destruct()
   {
      return SDB_OK ;
   }

   INT32 _sptDBNumberDecimal::cvtToBSON( const CHAR* key, const sptObject &value,
                                         BOOLEAN isSpecialObj,
                                         BSONObjBuilder& builder,
                                         string &errMsg )
   {
      INT32 rc = SDB_OK ;
      if( isSpecialObj )
      {
         string decimalStr ;
         string precisionStr ;
         INT32 precision ;
         INT32 scale ;
         BOOLEAN appendSuccess = FALSE ;
         rc = value.getStringField( SPT_NUMBERDECIMAL_SPECIALOBJ_DECIMAL_FIELD,
                                    decimalStr ) ;
         if( SDB_OK != rc )
         {
            errMsg = "Decimal $decimal must be string" ;
            goto error ;
         }
         if( value.isFieldExist( SPT_NUMBERDECIMAL_SPECIALOBJ_PRECISION_FIELD ) )
         {
            rc = value.getStringField( SPT_NUMBERDECIMAL_SPECIALOBJ_PRECISION_FIELD,
                                       precisionStr,
                                       SPT_CVT_FLAGS_FROM_OBJECT ) ;
            if( SDB_OK != rc )
            {
               errMsg = "Decimal $precision must be obj" ;
               goto error ;
            }
            rc = _getDecimalPrecision( precisionStr.c_str(), &precision,
                                       &scale ) ;
            if ( SDB_OK != rc )
            {
               errMsg = "Failed to conversion Decimal" ;
               goto error ;
            }
            appendSuccess = builder.appendDecimal( key, decimalStr, precision, scale ) ;
         }
         else
         {
            appendSuccess = builder.appendDecimal( key, decimalStr ) ;
         }
         if( FALSE == appendSuccess )
         {
            rc = SDB_INVALIDARG ;
            errMsg = "Failed to append deciaml" ;
            goto error ;
         }
      }
      else
      {
         rc = SDB_SYS ;
         errMsg = "Decimal must be special obj" ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptDBNumberDecimal::fmpToBSON( const sptObject &value,
                                         BSONObj &retObj, string &errMsg )
   {
      errMsg = "Decimal must be special obj" ;
      return SDB_SYS ;
   }

   INT32 _sptDBNumberDecimal::bsonToJSObj( sdbclient::sdb &db,
                                           const BSONObj &data,
                                           _sptReturnVal &rval,
                                           bson::BSONObj &detail )
   {
      detail = BSON( SPT_ERR << "Decimal must be special obj" ) ;
      return SDB_SYS ;
   }

   INT32 _sptDBNumberDecimal::_getDecimalPrecision( const CHAR *precisionStr,
                                                    INT32 *precision,
                                                    INT32 *scale )
   {
      //precisionStr:10,6
      BOOLEAN isFirst = TRUE ;
      INT32 rc        = SDB_OK ;
      const CHAR *p   = precisionStr ;
      while ( NULL != p && '\0' != *p )
      {
         if ( ' ' == *p )
         {
            p++ ;
            continue ;
         }

         if ( ',' == *p )
         {
            if ( !isFirst )
            {
               rc = SDB_INVALIDARG ;
               goto error ;
            }

            isFirst = FALSE ;
            p++ ;
            continue ;
         }

         if ( *p < '0' || *p > '9' )
         {
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         p++ ;
      }
      rc = sscanf ( precisionStr, "%d,%d", precision, scale ) ;
      if ( 2 != rc )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      rc = SDB_OK ;
   done:
      return rc ;
   error:
      goto done ;
   }
}

