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

   Source File Name = sptDBQueryOption.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          27/07/2018  ZWB  Initial Draft

   Last Changed =

*******************************************************************************/
#include "sptDBQueryOption.hpp"
#include "sptBsonobj.hpp"

using namespace bson ;

namespace engine
{

   JS_CONSTRUCT_FUNC_DEFINE( _sptDBQueryOption, construct )
   JS_DESTRUCT_FUNC_DEFINE( _sptDBQueryOption, destruct )

   JS_BEGIN_MAPPING_WITHPARENT( _sptDBQueryOption, SPT_QUERYOPTION_NAME,
                                _sptDBOptionBase )
      JS_ADD_CONSTRUCT_FUNC( construct )
      JS_ADD_DESTRUCT_FUNC( destruct )
      JS_SET_CVT_TO_BSON_FUNC( _sptDBQueryOption::cvtToBSON )
      JS_SET_JSOBJ_TO_BSON_FUNC( _sptDBQueryOption::fmpToBSON )
      JS_SET_BSON_TO_JSOBJ_FUNC( _sptDBQueryOption::bsonToJSObj )
   JS_MAPPING_END()

   _sptDBQueryOption::_sptDBQueryOption()
   {
   }

   _sptDBQueryOption::~_sptDBQueryOption()
   {
   }

   INT32 _sptDBQueryOption::construct( const _sptArguments &arg,
                                       _sptReturnVal &rval,
                                       bson::BSONObj &detail )
   {
      _sptDBOptionBase::construct( arg, rval, detail ) ;

      rval.addSelfProperty( SPT_QUERYOPTION_OPTIONS_FIELD,
                            SPT_PROP_ENUMERATE )->setValue( BSONObj() ) ;

      return SDB_OK ;
   }

   INT32 _sptDBQueryOption::destruct()
   {
      _sptDBOptionBase::destruct() ;
      return SDB_OK ;
   }

   INT32 _sptDBQueryOption::cvtToBSON( const CHAR* key,
                                       const sptObject &value,
                                       BOOLEAN isSpecialObj,
                                       BSONObjBuilder& builder,
                                       string &errMsg )
   {
      INT32 rc = SDB_OK ;
      BSONObj obj ;

      rc = fmpToBSON( value, obj, errMsg ) ;
      if ( rc )
      {
         goto error ;
      }

      builder.append( key, obj ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptDBQueryOption::fmpToBSON( const sptObject &value,
                                       BSONObj &retObj,
                                       string &errMsg )
   {
      INT32 rc = SDB_OK ;
      sptObjectPtr ptr ;
      sptBsonobj *pBsonObj = NULL ;

      BSONObj baseRetObj ;
      BSONObjBuilder builder ;

      rc = _sptDBOptionBase::fmpToBSON( value, baseRetObj, errMsg ) ;
      if ( rc )
      {
         goto error ;
      }

      builder.appendElements( baseRetObj ) ;

      if ( value.isFieldExist( SPT_QUERYOPTION_OPTIONS_FIELD ) )
      {
         rc = value.getObjectField( SPT_QUERYOPTION_OPTIONS_FIELD,
                                    ptr ) ;
         if ( rc )
         {
            errMsg = "Failed to get options field" ;
            goto error ;
         }

         rc = ptr->getUserObj( _sptBsonobj::__desc,
                               (const void**)&pBsonObj ) ;
         if ( rc )
         {
            errMsg = "Failed to get option data field" ;
            goto error ;
         }
         builder.append( SPT_QUERYOPTION_OPTIONS_FIELD,
                         pBsonObj->getBson() ) ;
      }

      retObj = builder.obj() ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _sptDBQueryOption::bsonToJSObj( sdbclient::sdb &db,
                                         const BSONObj &data,
                                         _sptReturnVal &rval,
                                         bson::BSONObj &detail )
   {
      INT32 rc = SDB_OK ;
      sptDBQueryOption *pSnapOption = NULL ;

      pSnapOption = SDB_OSS_NEW sptDBQueryOption() ;
      if( NULL == pSnapOption )
      {
         rc = SDB_OOM ;
         detail = BSON( SPT_ERR << "Failed to new sptDBQueryOption obj" ) ;
         goto error ;
      }

      rc = rval.setUsrObjectVal< sptDBQueryOption >( pSnapOption ) ;
      if( SDB_OK != rc )
      {
         detail = BSON( SPT_ERR << "Failed to set ret obj" ) ;
         goto error ;
      }

      _setReturnVal( data, rval ) ;

   done:
      return rc ;
   error:
      SAFE_OSS_DELETE( pSnapOption ) ;
      goto done ;
   }

   void _sptDBQueryOption::_setReturnVal( const BSONObj &data,
                                          _sptReturnVal &rval )
   {
      _sptDBOptionBase::_setReturnVal(  data, rval ) ;

      BSONObj obj ;
      obj = data.getObjectField( SPT_QUERYOPTION_OPTIONS_FIELD ) ;
      rval.addSelfProperty( SPT_QUERYOPTION_OPTIONS_FIELD,
                            SPT_PROP_ENUMERATE )->setValue( obj ) ;
   }

}
