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

   Source File Name = dpsTransExecutor.hpp

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

#ifndef DPS_TRANS_EXECUTOR_HPP__
#define DPS_TRANS_EXECUTOR_HPP__

#include "sdbInterface.hpp"
#include "dpsTransLockDef.hpp"
#include "dpsTransDef.hpp"
#include "dpsTransLockMgr.hpp"
#include "utilSegment.hpp"
#include "ossMemPool.hpp"
#include "dpsDef.hpp"

using namespace bson ;
using namespace std ;

namespace engine
{

   class dpsTransLRBHeader;

   /*
      DPS_TRANS_QUE_TYPE define
   */
   enum DPS_TRANS_QUE_TYPE
   {
      DPS_QUE_NULL         = 0,
      DPS_QUE_UPGRADE,
      DPS_QUE_WAITER
   } ;

   /*
      _dpsTransExecutor define
   */
   class _dpsTransExecutor
   {
      struct cmpCSCLLock
      {
         bool operator() ( const dpsTransLockId& lhs, 
                           const dpsTransLockId& rhs ) const 
         {
            if ( lhs.csID() < rhs.csID() )
            {
               return TRUE ;
            }
            else if ( lhs.csID() > rhs.csID() )
            {
               return FALSE ;
            }
            if ( lhs.clID() < rhs.clID() )
            {
               return TRUE ;
            }
            else if ( lhs.clID() > rhs.clID() )
            {
               return FALSE ;
            }
            return FALSE ;
         }
      };

      // Only CS and CL lock should be inserted in this map. If other locks
      // are to be inserted, the cmpCSCLLock compare function needs to be updated
      typedef ossPoolMap< dpsTransLockId,
                          dpsTransLRB*, 
                          cmpCSCLLock >                  DPS_LOCKID_MAP ;
      typedef DPS_LOCKID_MAP::iterator                   DPS_LOCKID_MAP_IT ;
      typedef DPS_LOCKID_MAP::const_iterator             DPS_LOCKID_MAP_CIT ;

      typedef ossPoolMap<DPS_LSN_OFFSET,dmsRecordID>     MAP_LSN_2_RECORD ;
      typedef MAP_LSN_2_RECORD::iterator                 MAP_LSN_2_RECORD_IT ;
      typedef MAP_LSN_2_RECORD::const_iterator           MAP_LSN_2_RECORD_CIT ;

      friend class _pmdEDUCB ;

      public:
         _dpsTransExecutor() ;
         virtual ~_dpsTransExecutor() ;

         void     clearAll() ;
         void     assertLocks() ;

      public:

         void                 setWaiterInfo( dpsTransLRB * lrb,
                                             DPS_TRANS_QUE_TYPE type ) ;
         void                 clearWaiterInfo() ;

         dpsTransLRB*         getWaiterLRB() const ;
         DPS_TRANS_QUE_TYPE   getWaiterQueType() const ;

         void                 setLastLRB( dpsTransLRB *lrb ) ;
         void                 clearLastLRB() ;
         dpsTransLRB *          getLastLRB() const ;

         BOOLEAN              addLock( const dpsTransLockId &lockID,
                                       dpsTransLRB * lrb ) ;
         BOOLEAN              findLock( const dpsTransLockId &lockID,
                                        dpsTransLRB * &lrb ) const ;
         BOOLEAN              removeLock( const dpsTransLockId &lockID ) ;
         void                 clearLock() ;

         void                 incLockCount() ;
         void                 decLockCount() ;
         void                 clearLockCount() ;
         UINT32               getLockCount() const ;

         /*
            Transaction Related
         */
         INT32                getTransIsolation() const ;
         UINT32               getTransTimeout() const ;
         BOOLEAN              isTransWaitLock() const ;
         BOOLEAN              useRollbackSegment() const ;
         BOOLEAN              useTransLock() const ;

         UINT32               getTransConfMask() const ;

         void                 setTransIsolation( INT32 isolation,
                                                 BOOLEAN enableMask = TRUE ) ;
         void                 setTransTimeout( UINT32 timeout,
                                               BOOLEAN enableMask = TRUE ) ;
         void                 setTransWaitLock( BOOLEAN waitLock,
                                                BOOLEAN enableMask = TRUE ) ;
         void                 setUseRollbackSemgent( BOOLEAN use,
                                                     BOOLEAN enableMask = TRUE ) ;

         void                 setUseTransLock( BOOLEAN use ) ;

         /*
            LSN to record map functions
         */
         const MAP_LSN_2_RECORD*    getRecordMap() const ;
         void                       putRecord( DPS_LSN_OFFSET lsnOffset,
                                               const dmsRecordID &item ) ;
         void                       delRecord( DPS_LSN_OFFSET lsnOffset ) ;
         BOOLEAN                    getRecord( DPS_LSN_OFFSET lsnOffset,
                                               dmsRecordID &item,
                                               BOOLEAN withDel = FALSE ) ;
         void                       clearRecordMap() ;
         BOOLEAN                    isRecordMapEmpty() const ;
         UINT32                     getRecordMapSize() const ;

      protected:
         void                 initTransConf( INT32 isolation,
                                             UINT32 timeout,
                                             BOOLEAN waitLock ) ;

         void                 updateTransConf( INT32 isolation,
                                               UINT32 timeout,
                                               BOOLEAN waitLock ) ;

      public:
         /*
            Interface
         */
         virtual EDUID        getEDUID() const = 0 ;
         virtual void         wakeup() = 0 ;
         virtual INT32        wait( INT64 timeout ) = 0 ;
         virtual IExecutor*   getExecutor() = 0 ;

      protected:
         dpsTransLRB *           _waiter ;
         DPS_TRANS_QUE_TYPE      _waiterQueType ;
         dpsTransLRB *           _lastLRB ;

         DPS_LOCKID_MAP          _mapCSCLLockID ;
         UINT32                  _lockCount ;

         /*
            LSN to record info
         */
         MAP_LSN_2_RECORD        _mapLSN2Record ;

      private:
         /// transaction configs
         INT32                   _transIsolation ;
         UINT32                  _transTimeout ;      /// Unit:ms
         // if transaction wait for lock
         BOOLEAN                 _transWaitLock ;  
         // if transaction use old copy in rollback segment
         BOOLEAN                 _useRollbackSegment ;
         UINT32                  _transConfMask ;

         BOOLEAN                 _useTransLock ;

   } ;
   typedef _dpsTransExecutor dpsTransExecutor ;

}

#endif // DPS_TRANS_EXECUTOR_HPP__

