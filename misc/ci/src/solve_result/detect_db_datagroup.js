/*******************************************************************************
*@Description: JavaScript common function library
*@Modify list:
*   2014-2-24 Jianhui Xu  Init
*******************************************************************************/

// begin global variable configuration
// CSPREFIX, COORDSVCNAME, COORDHOSTNAME  is input parameter
// UUID, UUNAME is input parameter
//if ( typeof(CSPREFIX) == "undefined" ) { CSPREFIX = "local_test"; }
if ( typeof(CMSVCNAME) == "undefined" ) { CMSVCNAME = "11790"; }
if ( typeof(CHANGEDPREFIX) == "undefined" ) { CHANGEDPREFIX = "local_test"; }
if ( typeof(COORDSVCNAME) == "undefined" ) { COORDSVCNAME = "50000"; }
if ( typeof(CATASVCNAME) == "undefined" ) { CATASVCNAME = "11800"; }
if ( typeof(COORDHOSTNAME) == "undefined" ) { COORDHOSTNAME = 'localhost'; }
if ( typeof(UUID) == "undefined" ) { UUID = 1 ; }
if ( typeof(UUNAME) == "undefined" ) { UUNAME = "ID"+UUID+"NAME" ; }
if ( typeof(RSRVPORTBEGIN) == "undefined" ) 	{ RSRVPORTBEGIN = '26000'; }
if ( typeof(RSRVPORTEND)   == "undefined" ) 	{ RSRVPORTEND   = '27000'; }
if ( typeof(RSRVNODEDIR)   == "undefined" ) 	{ RSRVNODEDIR   = "/opt/sequoiadb/database/"; }

var COMMCSNAME = CHANGEDPREFIX + "_cs" ;
var COMMCLNAME = CHANGEDPREFIX + "_cl" ;
var COMMDUMMYCLNAME = "test_dummy_cl" ;

var DATA_GROUP_ID_BEGIN = 1000 ;
var CATALOG_GROUPNAME = "SYSCatalogGroup" ;
var COORD_GROUPNAME = "SYSCoord" ;
var DOMAINNAME = "_SYS_DOMAIN_" ;
var SPARE_GROUPNAME = "SYSSpare";
var CATALOG_GROUPID = 1 ;
var COORD_GROUPID = 2 ;
var SPARE_GROUPID = 5 ;
// end global variable configuration

// control variable
var funcCommDropCSTimes = 0 ;
var funcCommCreateCSTimes = 0 ;
var funcCommCreateCLTimes = 0 ;
var funcCommCreateCLOptTimes =  0 ;
var funcCommDropCLTimes = 0 ;
// end control variable

var db = new Sdb( COORDHOSTNAME, COORDSVCNAME ) ;
function main( db )
{
   // 1. check nodes
   var groups = commGetGroups( db, "", "", false ) ;
   var errNodes = commCheckBusiness( groups ) ;
   if ( errNodes.length == 0 )
   {
   }
   else
   {
      println( "Has " + errNodes.length + " nodes in fault before test-case: " ) ;
   }
}

try
{
   main( db ) ;
}
catch( e )
{
   println( "End all test-case environment detect failed: " + e ) ;
   throw e;
}

/* *****************************************************************************
@discription: get all groups
@author: Jianhui Xu
@parameter:
   filter: group name filter
   exceptCata : default true
   exceptCoord: default true
@return array[][] ex:
        [0]
           [0] {"GroupName":"XXXX", "GroupID":XXXX, "Status":XX, "Version":XX, "Role":XX, "PrimaryNode":XXXX, "Length":XXXX, "PrimaryPos":XXXX}
           [1] {"HostName":"XXXX", "dbpath":"XXXX", "svcname":"XXXX", "NodeID":XXXX}
           [N] ...
        [N]
           ...
***************************************************************************** */
function commGetGroups( db, print, filter, exceptCata, exceptCoord, exceptSpare )
{ 
   
   if ( filter == undefined ) { filter = "" ; }
   if ( undefined == print ) { print = false ; }
   if ( undefined == exceptCata ) { exceptCata = true ; }
   if ( undefined == exceptCoord ) { exceptCoord = true ;}
   if ( undefined == exceptSpare ) { exceptSpare = true ;}
   
   var tmpArray = new Array() ;
   var tmpInfo ;
   try
   {
      tmpInfo = db.listReplicaGroups().toArray() ;
   }
   catch( e )
   {
      if ( e == -159 )
      {
         return tmpArray ;
      }
      else
      {
         if( true == print )
            println( "commGetGroups failed: " + e ) ;
         throw e ;
      }
   }

   var index = 0 ;
   for ( var i = 0 ; i < tmpInfo.length; ++i )
   {
      var tmpObj = eval( "(" + tmpInfo[i] + ")" ) ;
      if ( filter.length != 0 && tmpObj.GroupName.indexOf( filter, 0 ) == -1 )
      {
         continue ;
      }
      if ( true == exceptCata && tmpObj.GroupID == CATALOG_GROUPID )
      {
         continue ;
      }
      if ( true == exceptCoord && tmpObj.GroupID == COORD_GROUPID )
      {
         continue ;
      }
      if ( true == exceptSpare && tmpObj.GroupID == SPARE_GROUPID )
      {
         continue ;
      }
      tmpArray[index] = Array() ;
      tmpArray[index][0] = new Object() ;
      tmpArray[index][0].GroupName = tmpObj.GroupName ;         // GroupName
      tmpArray[index][0].GroupID = tmpObj.GroupID ;             // GroupID
      tmpArray[index][0].Status = tmpObj.Status ;               // Status
      tmpArray[index][0].Version = tmpObj.Version ;             // Version
      tmpArray[index][0].Role = tmpObj.Role ;                   // Role
      if ( typeof(tmpObj.PrimaryNode) == "undefined" )
      {
         tmpArray[index][0].PrimaryNode = -1 ;
      }
      else
      {
         tmpArray[index][0].PrimaryNode = tmpObj.PrimaryNode ;     // PrimaryNode
      }
      tmpArray[index][0].PrimaryPos = 0 ;                       // PrimaryPos

      var tmpGroupObj = tmpObj.Group ;
      tmpArray[index][0].Length = tmpGroupObj.length ;          // Length

      for ( var j = 0 ; j < tmpGroupObj.length; ++j )
      {
         var tmpNodeObj = tmpGroupObj[j] ;
         tmpArray[index][j+1] = new Object() ;
         tmpArray[index][j+1].HostName = tmpNodeObj.HostName ;           // HostName
         tmpArray[index][j+1].dbpath = tmpNodeObj.dbpath ;               // dbpath
         tmpArray[index][j+1].svcname = tmpNodeObj.Service[0].Name ;     // svcname
         tmpArray[index][j+1].NodeID = tmpNodeObj.NodeID ;

         if ( tmpNodeObj.NodeID == tmpObj.PrimaryNode )
         {
            tmpArray[index][0].PrimaryPos = j+1 ;                        // PrimaryPos
         }
      }
      ++index ;
   }

   return tmpArray ;
}

/* *****************************************************************************
@discription: check business right function
@author: Jianhui Xu
@return array[][] ex:
        [0]
           [0] {"GroupName":"XXXX", "GroupID":XXXX, "PrimaryNode":XXXX, "ConnCheck":t/f, "PrimaryCheck":t/f, "LSNCheck":t/f, "ServiceCheck":t/f, "DiskCheck":t/f }
           [1] {"HostName":"XXXX", "svcname":"XXXX", "NodeID":XXXX, "Connect":t/f, "IsPrimay":t/f, "LSN":XXXX, "ServiceStatus":t/f, "FreeSpace":XXXX }
           [N] ...
        [N]
           ...
***************************************************************************** */
function commCheckBusiness( groups, checkLSN, diskThreshold )
{
   if ( checkLSN == undefined ) { checkLSN = false ;}
   if ( diskThreshold == undefined ) { diskThreshold = 134217728 ; }
   var tmpCheckGrp = new Array() ;
   var grpindex = 0 ;
   var ndindex = 1 ;
   var error = false ;

   for ( var i = 0 ; i < groups.length; ++i )
   {
      error = false ;

      tmpCheckGrp[ grpindex ] = new Array() ;
      tmpCheckGrp[ grpindex ][0] = new Object() ;
      tmpCheckGrp[ grpindex ][0].GroupName = groups[i][0].GroupName ;
      tmpCheckGrp[ grpindex ][0].GroupID = groups[i][0].GroupID ;
      tmpCheckGrp[ grpindex ][0].PrimaryNode = groups[i][0].PrimaryNode ;
      tmpCheckGrp[ grpindex ][0].ConnCheck = true ;
      tmpCheckGrp[ grpindex ][0].PrimaryCheck = true ;
      tmpCheckGrp[ grpindex ][0].LSNCheck = true ;
      tmpCheckGrp[ grpindex ][0].ServiceCheck = true ;
      tmpCheckGrp[ grpindex ][0].DiskCheck = true ;

      // if no primary
      if ( groups[i][0].PrimaryNode == -1 )
      {
         error = true ;
         tmpCheckGrp[ grpindex ][0].PrimaryCheck = false ;
      }
      
      for ( var j = 1; j < groups[i].length; ++j )
      {
         ndindex = j ;
         tmpCheckGrp[grpindex][ndindex] = new Object() ;
         tmpCheckGrp[grpindex][ndindex].HostName = groups[i][j].HostName ;
         tmpCheckGrp[grpindex][ndindex].svcname = groups[i][j].svcname ;
         tmpCheckGrp[grpindex][ndindex].NodeID = groups[i][j].NodeID ;
         tmpCheckGrp[grpindex][ndindex].Connect = true ;
         tmpCheckGrp[grpindex][ndindex].IsPrimay = false ;
         tmpCheckGrp[grpindex][ndindex].LSN = -1 ;
         tmpCheckGrp[grpindex][ndindex].ServiceStatus = true ;
         tmpCheckGrp[grpindex][ndindex].FreeSpace = -1 ;

         var tmpDB ;
         // check connection
         try
         {
            tmpDB = new Sdb( groups[i][j].HostName, groups[i][j].svcname ) ;
         }
         catch ( e )
         {
            error = true ;
            tmpCheckGrp[ grpindex ][0].ConnCheck = false ;
            tmpCheckGrp[grpindex][ndindex].Connect = false ;
            continue ;
         }
         // check primary and lsn
         var tmpSysInfo ;
         try
         {
            tmpSysInfo = tmpDB.snapshot( 6 ).toArray() ;
         }
         catch( e )
         {
            error = true ;
            tmpCheckGrp[ grpindex ][0].ConnCheck = false ;
            tmpCheckGrp[grpindex][ndindex].Connect = false ;
            tmpDB.close() ;
            continue ;
         }
         //delete tmpCheckGrp[grpindex][ndindex].Connect ;
         // check primary
         var tmpSysInfoObj = eval( "(" + tmpSysInfo[0] + ")" ) ;
         if ( tmpSysInfoObj.IsPrimary == true )
         {
            tmpCheckGrp[grpindex][ndindex].IsPrimay = true ;
            if ( groups[i][0].PrimaryNode != groups[i][j].NodeID )
            {
               error = true ;
               tmpCheckGrp[ grpindex ][0].PrimaryCheck = false ;
            }
         }
         else
         {
            if ( groups[i][0].PrimaryNode == groups[i][j].NodeID )
            {
               error = true ;
               tmpCheckGrp[ grpindex ][0].PrimaryCheck = false ;
            }
            //delete tmpCheckGrp[grpindex][ndindex].IsPrimay ;
         }
         // check ServiceStatus
         if ( tmpSysInfoObj.ServiceStatus == false )
         {
            error = true ;
            tmpCheckGrp[ grpindex ][ndindex].ServiceStatus = false ;
            tmpCheckGrp[ grpindex ][0].ServiceCheck = false ;
         }
         // check disk
         tmpCheckGrp[grpindex][ndindex].FreeSpace = tmpSysInfoObj.Disk.FreeSpace ;
         if ( tmpCheckGrp[grpindex][ndindex].FreeSpace < diskThreshold )
         {
            error = true ;
            tmpCheckGrp[ grpindex ][0].DiskCheck = false ;
         }
         // check LSN
         tmpCheckGrp[ grpindex ][ndindex].LSN = tmpSysInfoObj.CurrentLSN.Offset ;
         if ( checkLSN && j > 1 && tmpCheckGrp[ grpindex ][ndindex].LSN != tmpCheckGrp[ grpindex ][1].LSN  )
         {
            error = true ;
            tmpCheckGrp[ grpindex ][0].LSNCheck = false ;
         }
         
         tmpDB.close() ;
      }
      
      if ( error == true )
      {
         ++grpindex ;
      }
   }
   // no error
   if ( error == false )
   {
      if ( grpindex == 0 )
      {
         tmpCheckGrp = new Array() ;
      }
      else
      {
         delete tmpCheckGrp[grpindex] ;
      }
   }
   return tmpCheckGrp ;
}