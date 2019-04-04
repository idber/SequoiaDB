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

   Source File Name = utilESCltFactory.cpp

   Descriptive Name = Elasticsearch client manager.

   When/how to use: this program may be used on binary and text-formatted
   versions of data management component. This file contains structure for
   DMS storage unit and its methods.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          14/04/2017  YSD Initial Draft

   Last Changed =

*******************************************************************************/
#include "core.hpp"
#include "pd.hpp"
#include "utilESClt.hpp"
#include "utilESCltFactory.hpp"

namespace seadapter
{
   _utilESCltFactory::_utilESCltFactory()
   {
      ossMemset( _url, 0, sizeof( _url ) ) ;
      _timeout = 0 ;
   }

   _utilESCltFactory::~_utilESCltFactory()
   {
   }

   INT32 _utilESCltFactory::init( const CHAR *url, INT32 timeout )
   {
      INT32 rc = SDB_OK ;

      if ( !url || ossStrlen( url ) > SEADPT_SE_SVCADDR_MAX_SZ )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG( PDERROR, "Url to init search engine client "
                          "manager is invalid" ) ;
         goto error ;
      }
      ossStrncpy( _url, url, SEADPT_SE_SVCADDR_MAX_SZ + 1 ) ;
      _timeout = timeout ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _utilESCltFactory::create( utilESClt **seClt )
   {
      INT32 rc = SDB_OK ;
      utilESClt* client = NULL ;

      SDB_ASSERT( seClt, "Parameter should not be null" ) ;

      client = SDB_OSS_NEW _utilESClt() ;
      if ( !client )
      {
         rc = SDB_OOM ;
         PD_LOG( PDERROR, "Failed to allocate memory for search engine client, "
                 "rc: %d", rc ) ;
         goto error ;
      }
      rc = client->init( _url, FALSE, _timeout ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to init search engine client, rc: %d",
                 rc ) ;
         goto error ;
      }

      *seClt = client ;

   done:
      return rc ;
   error:
      if ( client )
      {
         SDB_OSS_DEL client ;
      }
      goto done ;
   }
}

