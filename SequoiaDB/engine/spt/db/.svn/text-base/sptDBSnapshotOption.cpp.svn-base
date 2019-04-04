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

   Source File Name = sptDBSnapshotOption.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          16/07/2018  ZWB  Initial Draft

   Last Changed =

*******************************************************************************/
#include "sptDBSnapshotOption.hpp"
#include "sptBsonobj.hpp"

using namespace bson ;

namespace engine
{

   JS_CONSTRUCT_FUNC_DEFINE( _sptDBSnapshotOption, construct )
   JS_DESTRUCT_FUNC_DEFINE( _sptDBSnapshotOption, destruct )

   JS_BEGIN_MAPPING_WITHPARENT( _sptDBSnapshotOption, SPT_SNAPSHOTOPTION_NAME,
                                _sptDBOptionBase )

      JS_ADD_CONSTRUCT_FUNC( construct )
      JS_ADD_DESTRUCT_FUNC( destruct )
      JS_SET_CVT_TO_BSON_FUNC( _sptDBOptionBase::cvtToBSON )
      JS_SET_JSOBJ_TO_BSON_FUNC( _sptDBOptionBase::fmpToBSON )
      JS_SET_BSON_TO_JSOBJ_FUNC( _sptDBSnapshotOption::bsonToJSObj )
   JS_MAPPING_END()

   _sptDBSnapshotOption::_sptDBSnapshotOption()
   {
   }

   _sptDBSnapshotOption::~_sptDBSnapshotOption()
   {
   }

   INT32 _sptDBSnapshotOption::construct( const _sptArguments &arg,
                                          _sptReturnVal &rval,
                                          bson::BSONObj &detail )
   {
      _sptDBOptionBase::construct( arg, rval, detail ) ;

      return SDB_OK ;
   }

   INT32 _sptDBSnapshotOption::destruct()
   {
      _sptDBOptionBase::destruct() ;
      return SDB_OK ;
   }

   INT32 _sptDBSnapshotOption::bsonToJSObj( sdbclient::sdb &db,
                                            const BSONObj &data,
                                            _sptReturnVal &rval,
                                            bson::BSONObj &detail )
   {
      INT32 rc = SDB_OK ;
      _sptDBSnapshotOption *pSnapOption = NULL ;

      pSnapOption = SDB_OSS_NEW _sptDBSnapshotOption() ;
      if( NULL == pSnapOption )
      {
         rc = SDB_OOM ;
         detail = BSON( SPT_ERR << "Failed to new _sptDBSnapshotOption obj" ) ;
         goto error ;
      }

      rc = rval.setUsrObjectVal< _sptDBSnapshotOption >( pSnapOption ) ;
      if( SDB_OK != rc )
      {
         detail = BSON( SPT_ERR << "Failed to set ret obj" ) ;
         goto error ;
      }

      _sptDBOptionBase::_setReturnVal( data, rval ) ;

   done:
      return rc ;
   error:
      SAFE_OSS_DELETE( pSnapOption ) ;
      goto done ;
   }


}
