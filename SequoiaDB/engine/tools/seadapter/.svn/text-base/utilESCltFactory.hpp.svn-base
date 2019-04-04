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

   Source File Name = utilESCltFactory.hpp

   Descriptive Name = Elasticsearch client factory

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
#ifndef UTIL_SECLT_FACTORY_HPP_
#define UTIL_SECLT_FACTORY_HPP_

#include "ossLatch.hpp"
#include "utilESClt.hpp"
#include "seAdptDef.hpp"

namespace seadapter
{
   // Factory for search engine client.
   class _utilESCltFactory : public SDBObject
   {
   public:
      _utilESCltFactory() ;
      ~_utilESCltFactory() ;

      INT32 init( const CHAR *url, INT32 timeout ) ;
      INT32 create( utilESClt **seClt ) ;

   private:
      CHAR  _url[ SEADPT_SE_SVCADDR_MAX_SZ + 1 ] ;  // Search engine address
      INT32 _timeout ;
   } ;
   typedef _utilESCltFactory utilESCltFactory ;
}

#endif /* UTIL_SECLT_FACTORY_HPP_ */

