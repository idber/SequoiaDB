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

   Source File Name = utilCipherFile.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          11/26/2018  ZWB  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef UTILCIPHERFILE_H_
#define UTILCIPHERFILE_H_

#include "ossFile.hpp"

namespace engine
{

   class _utilCipherAbstractFile : public SDBObject
   {
   public:
      enum cipherRole
      {
         RRole,
         WRole
      } ;

      _utilCipherAbstractFile() {}
      virtual ~_utilCipherAbstractFile() {}

      virtual INT32 initFile( const std::string &fileName, 
                              cipherRole role ) = 0 ;
      virtual INT32 readFromFile( const CHAR **fileContent,
                                  INT64 *contentLen ) = 0 ;
      virtual INT32 writeToFile( const std::string& fileContent ) = 0 ;
   } ;
   typedef _utilCipherAbstractFile utilCipherAbstractFile ;


   class _utilCipherFile : public _utilCipherAbstractFile
   {
   public:
      _utilCipherFile() : _fileContent( NULL ) {}
      ~_utilCipherFile() ;

      INT32 initFile( const std::string &fileName, 
                      cipherRole role) ;
      INT32 readFromFile( const CHAR **fileContent,
                          INT64 *contentLen ) ;
      INT32 writeToFile( const std::string& fileContent ) ;
   private:
      ossFile  _file ;
      CHAR    *_fileContent ;
   } ;
   typedef _utilCipherFile utilCipherFile ;

}

#endif