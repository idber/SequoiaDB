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

   Source File Name = coordCommandSequence.cpp

   Descriptive Name = Coordinator Sequence Command

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          08/06/2018  David Li  Initial Draft

   Last Changed =

*******************************************************************************/
#include "coordCommandSequence.hpp"
#include "coordSequenceAgent.hpp"
#include "coordCB.hpp"
#include "coordResource.hpp"
#include "msgMessage.hpp"

using namespace bson ;

namespace engine
{
   /*
      _coordCMDInvalidateSequenceCache implement
   */
   COORD_IMPLEMENT_CMD_AUTO_REGISTER( _coordCMDInvalidateSequenceCache,
                                      CMD_NAME_INVALIDATE_SEQUENCE_CACHE,
                                      TRUE ) ;
   _coordCMDInvalidateSequenceCache::_coordCMDInvalidateSequenceCache()
   {
   }

   _coordCMDInvalidateSequenceCache::~_coordCMDInvalidateSequenceCache()
   {
   }

   void _coordCMDInvalidateSequenceCache::_preSet( pmdEDUCB * cb,
                                                   coordCtrlParam & ctrlParam )
   {
      ctrlParam._isGlobal = TRUE ;
      ctrlParam._filterID = FILTER_ID_MATCHER ;
      ctrlParam._emptyFilterSel = NODE_SEL_ALL ;
      ctrlParam.resetRole() ;
      ctrlParam._role[ SDB_ROLE_COORD ] = 1 ;
   }

   UINT32 _coordCMDInvalidateSequenceCache::_getControlMask() const
   {
      return COORD_CTRL_MASK_GLOBAL ;
   }

   IMPLEMENT_CMD_AUTO_REGISTER(_coordInvalidateSequenceCache)
   _coordInvalidateSequenceCache::_coordInvalidateSequenceCache()
   {
      _sequenceID = UTIL_SEQUENCEID_NULL ;
   }

   _coordInvalidateSequenceCache::~_coordInvalidateSequenceCache()
   {

   }

   INT32 _coordInvalidateSequenceCache::spaceNode()
   {
      return CMD_SPACE_NODE_COORD ;
   }

   INT32 _coordInvalidateSequenceCache::init ( INT32 flags,
                                               INT64 numToSkip,
                                               INT64 numToReturn,
                                               const CHAR *pMatcherBuff,
                                               const CHAR *pSelectBuff,
                                               const CHAR *pOrderByBuff,
                                               const CHAR *pHintBuff )
   {
      INT32 rc = SDB_OK ;

      try
      {
         BSONObj obj( pMatcherBuff ) ;
         BSONElement e ;

         // check sequence name
         e = obj.getField( FIELD_NAME_SEQUENCE_NAME ) ;
         if ( String == e.type() )
         {
            _sequenceName = e.String() ;
         }
         else if ( !e.eoo() )
         {
            PD_LOG( PDERROR, "Field[%s] is invalid in obj[%s]",
                    FIELD_NAME_SEQUENCE_NAME,
                    obj.toString( false, false).c_str() ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         else
         {
            PD_LOG( PDERROR, "Missing field[%s] in obj[%s]",
                    FIELD_NAME_SEQUENCE_NAME,
                    obj.toString( false, false ).c_str() ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
         if( obj.hasField( FIELD_NAME_SEQUENCE_ID ) )
         {
            e = obj.getField( FIELD_NAME_SEQUENCE_ID ) ;
            PD_CHECK( e.isNumber(), SDB_INVALIDARG, error, PDERROR,
                      "Field[%s] is invalid in obj[%s]",
                     FIELD_NAME_SEQUENCE_ID,
                     obj.toString( false, false ).c_str() ) ;
            _sequenceID = e.Long() ;
         }
      }
      catch( std::exception &e )
      {
         PD_LOG( PDERROR, "Occur exception: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _coordInvalidateSequenceCache::doit ( _pmdEDUCB *cb,
                                               SDB_DMSCB *dmsCB,
                                               _SDB_RTNCB *rtnCB,
                                               _dpsLogWrapper *dpsCB,
                                               INT16 w,
                                               INT64 *pContextID )
   {
      coordSequenceAgent* sequenceAgent =
         sdbGetCoordCB()->getResource()->getSequenceAgent() ;
      sequenceAgent->removeCache( _sequenceName, _sequenceID ) ;
      return  SDB_OK ;
   }
}

