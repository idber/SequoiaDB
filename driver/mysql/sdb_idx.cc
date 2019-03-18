#ifndef MYSQL_SERVER
   #define MYSQL_SERVER
#endif

#include "sdb_idx.h"

const char * sdb_get_idx_name( KEY * key_info )
{
   if ( key_info )
   {
      return key_info->name ;
   }
   return NULL ;
}
