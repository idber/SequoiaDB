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

   Source File Name = seAdptOptionsMgr.hpp

   Descriptive Name = Search engine adapter options manager

   When/how to use: this program may be used on binary and text-formatted
   versions of data management component. This file contains structure for
   DMS storage unit and its methods.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          04/08/2017  YSD Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef SEADPT_OPTIONSMGR_HPP__
#define SEADPT_OPTIONSMGR_HPP__

#include "pmdOptionsMgr.hpp"

namespace seadapter
{
   // Manage the configurations of the search engine adapter
   class _seAdptOptionsMgr : public engine::_pmdCfgRecord
   {
   public:
      _seAdptOptionsMgr() ;
      virtual ~_seAdptOptionsMgr() {}

      INT32 init( INT32 argc, CHAR **argv, const CHAR *exePath ) ;

      void setSvcName( const CHAR *svcName ) ;
      const CHAR* getCfgFileName() const { return _cfgFileName ; }
      const CHAR* getSvcName() const { return _serviceName ; }
      const CHAR* getDBHost() const { return _dbHost ; }
      const CHAR* getDBService() const { return _dbService ; }
      const CHAR* getSEHost() const { return _seHost ; }
      const CHAR* getSEService() const { return _seService ; }
      PDLEVEL     getDiagLevel() const ;
      INT32       getTimeout() const ;
      UINT32      getBulkBuffSize() const ;

   protected:
      virtual INT32 doDataExchange( engine::pmdCfgExchange *pEX ) ;

   private:
      CHAR     _cfgFileName[ OSS_MAX_PATHSIZE + 1 ] ;
      CHAR     _serviceName[ OSS_MAX_SERVICENAME + 1 ] ;
      CHAR     _dbHost[ OSS_MAX_HOSTNAME + 1 ] ;
      CHAR     _dbService[ OSS_MAX_SERVICENAME + 1 ] ;
      CHAR     _seHost[ OSS_MAX_PATHSIZE + 1 ] ;
      CHAR     _seService[ OSS_MAX_SERVICENAME + 1 ] ;
      UINT16   _diagLevel ;
      INT32    _timeout ;
      UINT32   _bulkBuffSize ;
   } ;
   typedef _seAdptOptionsMgr seAdptOptionsMgr ;
}

#endif /* SEADPT_OPTIONSMGR_HPP__ */
