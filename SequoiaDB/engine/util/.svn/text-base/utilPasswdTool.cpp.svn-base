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

   Source File Name = utilPasswdTool.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          11/26/2018  ZWB  Initial Draft

   Last Changed =

*******************************************************************************/
#include "utilPasswdTool.hpp"
#include "linenoise.h"

namespace engine
{

   string _utilPasswordTool::interactivePasswdInput()
   {
      CHAR* line = NULL ;
      string passwd ;
      setEchoOff() ;
      if ( ( line = linenoise( "password:" ) ) != NULL )
      {
         passwd = line ;
         SDB_OSS_ORIGINAL_FREE( line ) ;
      }
      setEchoOn() ;
      return passwd ;
   }

   INT32 _utilPasswordTool::getPasswdByCipherFile( const string &user,
                                                   const string &token,
                                                   const string &cipherFile,
                                                   string &connectionUserName,
                                                   string &password )
   {
      INT32  rc = SDB_OK ;

      string filePath = cipherFile ;

      if ( filePath.empty() )
      {
         filePath = "./passwd" ;
      }

      _cipherfile.initFile( filePath, utilCipherFile::RRole ) ;

      rc = _cipherMgr.init( &_cipherfile ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      _cipherMgr.getConnectionUserName( user, connectionUserName ) ;

      rc = _cipherMgr.getPasswd( user, token, password ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

}