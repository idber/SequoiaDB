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

   Source File Name = utilCipherMgr.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          11/26/2018  ZWB  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef UTILCIPHERMGR_H_
#define UTILCIPHERMGR_H_

#include "utilCipherFile.hpp"
#include "ossTypes.hpp"
#include <string>
#include <map>
#include <vector>

namespace engine
{

   class _utilCipherMgr : public SDBObject
   {
   public:
      static const INT32  BYTES_PER_TIME = 8 ;
      static const INT32  KEY_BYTE_LENGTH = 8 ;
      static const INT32  RANDOM_ARRAY_BYTE_LENGTH = 16 ;
      static const INT32  UINT8_MAX_NUMBER = 65536 ;
      static const UINT32 INSERTABLE_MAX_LENGTH = 234 ;

      _utilCipherMgr() {}
      ~_utilCipherMgr() {}

      INT32 init( utilCipherAbstractFile *file ) ;
      INT32 addUser( const std::string &user, const std::string &token,
                     const std::string &passwd ) ;
      INT32 removeUser( const std::string &user ) ;
      INT32 getPasswd( const std::string &userInfo, const std::string &token,
                       std::string &passwd ) ;
      void  getConnectionUserName( const std::string &userInfo,
                                   std::string &connectionUserName ) ;

   private:

      INT32  _parseLine( std::string line, std::string& usr, std::string& cipherText ) ;
      INT32  _write( const std::string& fileContent ) ;
      void   _extractUserInfo( const std::string &userInfo, std::string &userName,
                               std::string &fullName ) ;
      INT32  _findCipherText( const std::string &userName, const std::string &fullName,
                              std::string &cipherText ) ;

   private:
      utilCipherAbstractFile *_cipherfile ;
      std::map<std::string, std::string> _usersCipher ;
   } ;
   typedef _utilCipherMgr utilCipherMgr ;

}

#endif // UTIL_CIPHER_HPP_