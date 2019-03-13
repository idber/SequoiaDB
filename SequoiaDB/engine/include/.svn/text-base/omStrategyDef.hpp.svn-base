/*******************************************************************************


   Copyright (C) 2011-2014 SequoiaDB Ltd.

   This program is free software: you can redistribute it and/or modify
   it under the term of the GNU Affero General Public License, version 3,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warrenty of
   MARCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program. If not, see <http://www.gnu.org/license/>.

   Source File Name = omStrategyDef.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          01/18/2016  Li Jianhua  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef OM_STRATEGY_DEF_HPP_
#define OM_STRATEGY_DEF_HPP_


#include "ossTypes.h"
#include "../bson/bson.h"
#include <boost/shared_ptr.hpp>


namespace engine
{
#define OM_TASK_STRATEGY_NICE_MAX                  19
#define OM_TASK_STRATEGY_NICE_MIN                  -20
#define OM_TASK_STRATEGY_NICE_DEF                  0
#define OM_TASK_STRATEGY_INVALID_VER               -1

   typedef struct _omTaskStrategyInfo
   {
   private:
      INT64                      taskID ;
   public:
      INT64                      _id  ;
      INT32                      nice ;
      std::string                taskName ;
      std::string                userName ;
      std::set<std::string>      ips ;

   public:
      friend class omStrategyMgr ;
      _omTaskStrategyInfo()
      {
         _id = 0 ;
         nice = 0 ;
         taskID = 0 ;
      }

      INT32 toBSON( bson::BSONObj &obj ) ;
      INT32 fromBSON( const bson::BSONObj &obj ) ;

      BOOLEAN isMatch( const std::string &userName, const std::string &ip ) ;

   protected:
      void setTaskID( INT64 newTaskID ) ;

   }omTaskStrategyInfo ;

   typedef boost::shared_ptr< omTaskStrategyInfo >          taskStrategyInfoPtr ;
}
#endif
