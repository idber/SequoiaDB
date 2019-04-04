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

   Source File Name = dpsTransExecutor.cpp

   Descriptive Name = Operating System Services Types Header

   When/how to use: this program may be used on binary and text-formatted
   versions of OSS component. This file contains declare for data types used in
   SequoiaDB.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          11/08/2018  XJH Initial Draft

   Last Changed =

*******************************************************************************/

#include "dpsTransExecutor.hpp"
#include "dpsTransLockDef.hpp"
#include "dpsTransCB.hpp"
#include "dpsTransLRB.hpp"

namespace engine
{

   /*
      _dpsTransExecutor implement
   */
   _dpsTransExecutor::_dpsTransExecutor()
   {
      _waiter           = NULL;
      _waiterQueType    = DPS_QUE_NULL ;
      _lastLRB          = NULL;
      _lockCount        = 0 ;

      _transIsolation   = DPS_TRANS_ISOLATION_DFT ;
      _transTimeout     = DPS_TRANS_DFT_TIMEOUT ;
      _transWaitLock    = DPS_TRANS_LOCKWAIT_DFT ;
      _useRollbackSegment = TRUE ;
      _transConfMask    = 0 ;

      _useTransLock     = TRUE ;
   }

   _dpsTransExecutor::~_dpsTransExecutor()
   {
   }

   void _dpsTransExecutor::clearAll()
   {
      clearWaiterInfo() ;
      clearLastLRB() ;
      clearLock() ;
      clearLockCount() ;
      clearRecordMap() ;
   }

   void _dpsTransExecutor::assertLocks()
   {
      SDB_ASSERT( _mapCSCLLockID.size() == 0, "Lock must be 0" ) ;
      SDB_ASSERT( _lockCount == 0, "Lock must be 0" ) ;
      SDB_ASSERT( _waiter == NULL,
                  "Waiter LRB must be invalid" ) ;
      SDB_ASSERT( _lastLRB == NULL,
                  "Last LRB must be invalid" ) ;
      SDB_ASSERT( isRecordMapEmpty(), "Record map must be empty" ) ;
   }

   void _dpsTransExecutor::setWaiterInfo( dpsTransLRB* waiter,
                                          DPS_TRANS_QUE_TYPE type )
   {
      _waiter        = waiter ;
      _waiterQueType = type ;
   }

   void _dpsTransExecutor::clearWaiterInfo()
   {
      _waiter        = NULL;
      _waiterQueType = DPS_QUE_NULL ;
   }

   dpsTransLRB* _dpsTransExecutor::getWaiterLRB() const
   {
      return _waiter ;
   }

   DPS_TRANS_QUE_TYPE _dpsTransExecutor::getWaiterQueType() const
   {
      return _waiterQueType ;
   }

   void _dpsTransExecutor::setLastLRB( dpsTransLRB* lrb )
   {
      _lastLRB = lrb ;
   }

   void _dpsTransExecutor::clearLastLRB()
   {
      _lastLRB = NULL ;
   }

   dpsTransLRB * _dpsTransExecutor::getLastLRB() const
   {
      return _lastLRB ;
   }

   BOOLEAN _dpsTransExecutor::addLock( const dpsTransLockId &lockID,
                                       dpsTransLRB * lrb )
   {
      // only add CS or CL lock into the map
      if ( ! lockID.isLeafLevel() )
      {
         if ( _mapCSCLLockID.insert( std::make_pair( lockID, lrb ) ).second )
         {
            return TRUE ;
         }
      }
      return FALSE ;
   }

   BOOLEAN _dpsTransExecutor::findLock( const dpsTransLockId &lockID,
                                        dpsTransLRB * &lrb ) const
   {
      // only search the map if it is CS or CL lock
      if ( ! lockID.isLeafLevel() )
      {
         DPS_LOCKID_MAP_CIT cit = _mapCSCLLockID.find( lockID ) ;
         if ( cit != _mapCSCLLockID.end() )
         {
            lrb = cit->second ;
            return TRUE ;
         }
      }
      return FALSE ;
   }

   BOOLEAN _dpsTransExecutor::removeLock( const dpsTransLockId &lockID )
   {
      // only do the remove if it is CS or CL lock
      if ( ! lockID.isLeafLevel() )
      {
         return _mapCSCLLockID.erase( lockID ) ? TRUE : FALSE ;
      }
      return FALSE ;
   }

   void _dpsTransExecutor::clearLock()
   {
      _mapCSCLLockID.clear() ;
   }

   void _dpsTransExecutor::incLockCount()
   {
      ++_lockCount ;
   }

   void _dpsTransExecutor::decLockCount()
   {
      SDB_ASSERT( _lockCount > 0, "Lock count must > 0" ) ;
      if ( _lockCount > 0 )
      {
         --_lockCount ;
      }
   }

   void _dpsTransExecutor::clearLockCount()
   {
      _lockCount = 0 ;
   }

   UINT32 _dpsTransExecutor::getLockCount() const
   {
      return _lockCount ;
   }

   INT32 _dpsTransExecutor::getTransIsolation() const
   {
      return _transIsolation ;
   }

   UINT32 _dpsTransExecutor::getTransTimeout() const
   {
      return _transTimeout ;
   }

   BOOLEAN _dpsTransExecutor::isTransWaitLock() const
   {
      return _transWaitLock ;
   }

   BOOLEAN _dpsTransExecutor::useRollbackSegment() const
   {
      return _useRollbackSegment ;
   }

   BOOLEAN _dpsTransExecutor::useTransLock() const
   {
      return _useTransLock ;
   }

   UINT32 _dpsTransExecutor::getTransConfMask() const
   {
      return _transConfMask ;
   }

   void _dpsTransExecutor::setTransIsolation( INT32 isolation,
                                              BOOLEAN enableMask )
   {
      if ( isolation >= TRANS_ISOLATION_RU &&
           isolation <= TRANS_ISOLATION_MAX - 1 )
      {
         _transIsolation = isolation ;
      }
      else
      {
         _transIsolation = DPS_TRANS_ISOLATION_DFT ;
      }

      if ( enableMask )
      {
         _transConfMask |= TRANS_CONF_MASK_ISOLATION ;
      }
   }

   void _dpsTransExecutor::setTransTimeout( UINT32 timeout,
                                            BOOLEAN enableMask )
   {
      _transTimeout = timeout ;

      if ( enableMask )
      {
         _transConfMask |= TRANS_CONF_MASK_TIMEOUT ;
      }
   }

   void _dpsTransExecutor::setTransWaitLock( BOOLEAN waitLock,
                                             BOOLEAN enableMask )
   {
      _transWaitLock = waitLock ;
      if ( enableMask )
      {
         _transConfMask |= TRANS_CONF_MASK_WAITLOCK ;
      }
   }

   void _dpsTransExecutor::setUseRollbackSemgent( BOOLEAN use,
                                                  BOOLEAN enableMask )
   {
      _useRollbackSegment = use ;
      if ( enableMask )
      {
         _transConfMask |= TRANS_CONF_MASK_USERBS ;
      }
   }

   void _dpsTransExecutor::setUseTransLock( BOOLEAN use )
   {
      _useTransLock = use ;
   }

   void _dpsTransExecutor::initTransConf( INT32 isolation,
                                          UINT32 timeout,
                                          BOOLEAN waitLock )
   {
      _transConfMask = 0 ;
      setTransIsolation( isolation, FALSE ) ;
      setTransTimeout( timeout, FALSE ) ;
      setTransWaitLock( waitLock, FALSE ) ;
      setUseRollbackSemgent( TRUE, FALSE ) ;

      _useTransLock        = TRUE ;
   }

   void _dpsTransExecutor::updateTransConf( INT32 isolation,
                                            UINT32 timeout,
                                            BOOLEAN waitLock )
   {
      if ( !OSS_BIT_TEST( _transConfMask, TRANS_CONF_MASK_ISOLATION ) )
      {
         setTransIsolation( isolation, FALSE ) ;
      }
      if ( !OSS_BIT_TEST( _transConfMask, TRANS_CONF_MASK_TIMEOUT ) )
      {
         setTransTimeout( timeout, FALSE ) ;
      }
      if ( !OSS_BIT_TEST( _transConfMask, TRANS_CONF_MASK_WAITLOCK ) )
      {
         setTransWaitLock( waitLock, FALSE ) ;
      }
      if ( !OSS_BIT_TEST( _transConfMask, TRANS_CONF_MASK_USERBS ) )
      {
         setUseRollbackSemgent( TRUE, FALSE ) ;
      }
   }

   const _dpsTransExecutor::MAP_LSN_2_RECORD*
      _dpsTransExecutor::getRecordMap() const
   {
      return &_mapLSN2Record ;
   }

   void _dpsTransExecutor::putRecord( DPS_LSN_OFFSET lsnOffset,
                                      const dmsRecordID &item )
   {
      pair<MAP_LSN_2_RECORD_IT,BOOLEAN> ret ;
      try
      {
         ret = _mapLSN2Record.insert( MAP_LSN_2_RECORD::value_type( lsnOffset,
                                                                    item ) ) ;
         if ( !ret.second )
         {
            SDB_ASSERT( FALSE, "Item must not been existed" ) ;
            ret.first->second = item ;
         }
      }
      catch ( std::exception &e )
      {
         PD_LOG( PDWARNING, "Occur exception: %s", e.what() ) ;
      }
   }

   void _dpsTransExecutor::delRecord( DPS_LSN_OFFSET lsnOffset )
   {
      _mapLSN2Record.erase( lsnOffset ) ;
   }

   BOOLEAN _dpsTransExecutor::getRecord( DPS_LSN_OFFSET lsnOffset,
                                         dmsRecordID &item,
                                         BOOLEAN withDel )
   {
      MAP_LSN_2_RECORD_IT it = _mapLSN2Record.find( lsnOffset ) ;
      if ( it != _mapLSN2Record.end() )
      {
         item = it->second ;
         if ( withDel )
         {
            _mapLSN2Record.erase( it ) ;
         }
         return TRUE ;
      }
      return FALSE ;
   }

   void _dpsTransExecutor::clearRecordMap()
   {
      _mapLSN2Record.clear() ;
   }

   BOOLEAN _dpsTransExecutor::isRecordMapEmpty() const
   {
      return _mapLSN2Record.empty() ? TRUE : FALSE ;
   }

   UINT32 _dpsTransExecutor::getRecordMapSize() const
   {
      return _mapLSN2Record.size() ;
   }

}

