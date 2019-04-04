/** @file bsonDecimal.cpp - BSON DECIMAL implementation
    http://www.mongodb.org/display/DOCS/BSON
*/

/*    Copyright 2009 10gen Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

#include "ossErr.h"
#include "ossMem.h"
#include "bsonDecimal.h"
#include "common_decimal_fun.h"

#ifdef SDB_ENGINE
#include "pd.hpp"
#else
   #ifdef _DEBUG
      #include <assert.h>
      #define SDB_ASSERT(cond,str)  assert(cond)
   #else
      #define SDB_ASSERT(cond,str)  
   #endif // _DEBUG
#endif

namespace bson {

   bsonDecimal::bsonDecimal()
   {
      init() ;
   }

   bsonDecimal::bsonDecimal( const bsonDecimal &right )
   {
      INT32 rc = SDB_OK ;
      init() ;
      rc = sdb_decimal_copy( &( right._decimal ), &_decimal ) ;
      SDB_ASSERT( SDB_OK == rc , "out of memory" ) ;
   }

   bsonDecimal::~bsonDecimal()
   {
      sdb_decimal_free( &_decimal ) ;
   }

   bsonDecimal& bsonDecimal::operator= ( const bsonDecimal &right )
   {
      INT32 rc = SDB_OK ;
      sdb_decimal_free( &_decimal ) ;
      rc = sdb_decimal_copy( &( right._decimal ), &_decimal ) ;
      SDB_ASSERT( SDB_OK == rc , "out of memory" ) ;

      return *this ;
   }

   INT32 bsonDecimal::init( INT32 precision, INT32 scale )
   {
      return sdb_decimal_init1( &_decimal, precision, scale ) ;
   }

   INT32 bsonDecimal::init()
   {
      sdb_decimal_init( &_decimal ) ;
      return SDB_OK ;
   }

   void bsonDecimal::setZero()
   {
      sdb_decimal_set_zero( &_decimal ) ;
   }

   BOOLEAN bsonDecimal::isZero()
   {
      return sdb_decimal_is_zero( &_decimal ) ;
   }

   void bsonDecimal::setMin()
   {
      sdb_decimal_set_min( &_decimal ) ;
   }

   BOOLEAN bsonDecimal::isMin()
   {
      return sdb_decimal_is_min( &_decimal ) ;
   }

   void bsonDecimal::setMax()
   {
      sdb_decimal_set_max( &_decimal ) ;
   }

   BOOLEAN bsonDecimal::isMax()
   {
      return sdb_decimal_is_max( &_decimal ) ;
   }

   INT32 bsonDecimal::fromInt( INT32 value )
   {
      return sdb_decimal_from_int( value, &_decimal ) ;
   }

   INT32 bsonDecimal::toInt( INT32 *value ) const 
   {
      if ( NULL == value )
      {
         return SDB_INVALIDARG ;
      }

      *value = sdb_decimal_to_int( &_decimal ) ;
      return SDB_OK ;
   }

   INT32 bsonDecimal::fromLong( INT64 value )
   {
      return sdb_decimal_from_long( value, &_decimal ) ;
   }

   INT32 bsonDecimal::toLong( INT64 *value ) const 
   {
      if ( NULL == value )
      {
         return SDB_INVALIDARG ;
      }

      *value = sdb_decimal_to_long( &_decimal ) ;
      return SDB_OK ;
   }

   INT32 bsonDecimal::fromDouble( FLOAT64 value )
   {
      return sdb_decimal_from_double( value, &_decimal ) ;
   }

   INT32 bsonDecimal::toDouble( FLOAT64 *value ) const 
   {
      if ( NULL == value )
      {
         return SDB_INVALIDARG ;
      }

      *value = sdb_decimal_to_double( &_decimal ) ;
      return SDB_OK ;
   }

   INT32 bsonDecimal::fromString( const CHAR *value )
   {
      return sdb_decimal_from_str( value, &_decimal ) ;
   }

   string bsonDecimal::toString() const 
   {
      INT32 rc       = SDB_OK ;
      CHAR *temp     = NULL ;
      INT32 size     = 0 ;
      string result  = "" ;

      rc = sdb_decimal_to_str_get_len( &_decimal, &size ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      temp = (CHAR *)SDB_OSS_MALLOC( size ) ;
      if ( NULL == temp )
      {
         goto error ;
      }

      rc = sdb_decimal_to_str( &_decimal, temp, size ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      result = temp ;

   done:
      if ( NULL != temp )
      {
         SDB_OSS_FREE( temp ) ;
      }
      return result ;
   error:
      goto done ;
   }

   string bsonDecimal::toJsonString()
   {
      INT32 rc       = SDB_OK ;
      CHAR *temp     = NULL ;
      INT32 size     = 0 ;
      string result  = "" ;

      rc = sdb_decimal_to_jsonstr_len( _decimal.sign, _decimal.weight, 
                                       _decimal.dscale, _decimal.typemod,
                                       &size ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      temp = (CHAR *)SDB_OSS_MALLOC( size ) ;
      if ( NULL == temp )
      {
         goto error ;
      }

      rc = sdb_decimal_to_jsonstr( &_decimal, temp, size ) ;
      if ( SDB_OK != rc )
      {
         goto error ;
      }

      result = temp ;

   done:
      if ( NULL != temp )
      {
         SDB_OSS_FREE( temp ) ;
      }
      return result ;
   error:
      goto done ;
   }

   INT32 bsonDecimal::fromBsonValue( const CHAR *bsonValue ) 
   {
      return sdb_decimal_from_bsonvalue( bsonValue, &_decimal ) ;
   }

   INT32 bsonDecimal::compare( const bsonDecimal &right ) const
   {
      return sdb_decimal_cmp( &_decimal, &( right._decimal ) ) ;
   }

   INT32 bsonDecimal::compare( int right ) const 
   {
      INT32 rc = SDB_OK ;
      bsonDecimal decimal ;

      rc = decimal.fromInt( right ) ;
      if ( SDB_OK != rc )
      {
         // always bigger than error
         return 1 ;
      }

      return compare( decimal ) ;
   }

   INT32 bsonDecimal::add( const bsonDecimal &right, bsonDecimal &result )
   {
      return sdb_decimal_add( &_decimal, &right._decimal, &result._decimal ) ;
   }

   INT32 bsonDecimal::add( const bsonDecimal &right )
   {
      INT32 rc      = SDB_OK ;
      INT32 typemod = -1 ;
      bsonDecimal result ;
      rc = add( right, result ) ;
      if ( SDB_OK != rc )
      {
         return rc ;
      }

      typemod = sdb_decimal_get_typemod2( &_decimal );

      rc = sdb_decimal_copy( &result._decimal, &_decimal ) ;
      if ( SDB_OK != rc )
      {
         return rc ;
      }
      
      return sdb_decimal_update_typemod( &_decimal, typemod ) ;
   }

   INT32 bsonDecimal::sub( const bsonDecimal &right, bsonDecimal &result )
   {
      return sdb_decimal_sub( &_decimal, &right._decimal, &result._decimal ) ;
   }

   INT32 bsonDecimal::mul( const bsonDecimal &right, bsonDecimal &result )
   {
      return sdb_decimal_mul( &_decimal, &right._decimal, &result._decimal ) ;
   }
   
   INT32 bsonDecimal::div( const bsonDecimal &right, bsonDecimal &result )
   {
      return sdb_decimal_div( &_decimal, &right._decimal, &result._decimal ) ;
   }

   INT32 bsonDecimal::div( INT64 right, bsonDecimal &result )
   {
      INT32 rc = SDB_OK ;
      bsonDecimal tmpRight ;
      rc = tmpRight.fromLong( right ) ;
      if ( SDB_OK != rc )
      {
         return rc ;
      }

      return div( tmpRight, result ) ;
   }

   INT32 bsonDecimal::abs()
   {
      return sdb_decimal_abs( &_decimal ) ;
   }

   INT32 bsonDecimal::ceil( bsonDecimal &result )
   {
      INT32 rc = SDB_OK ;
      rc = sdb_decimal_ceil( &_decimal, &(result._decimal) ) ;
      if ( SDB_OK != rc )
      {
         return rc ;
      }

      return sdb_decimal_update_typemod( &(result._decimal), -1 ) ;
   }

   INT32 bsonDecimal::floor( bsonDecimal &result )
   {
      INT32 rc = SDB_OK ;
      rc = sdb_decimal_floor( &_decimal, &(result._decimal) ) ;
      if ( SDB_OK != rc )
      {
         return rc ;
      }

      return sdb_decimal_update_typemod( &(result._decimal), -1 ) ;
   }

   INT32 bsonDecimal::mod( bsonDecimal &right, bsonDecimal &result )
   {
      INT32 rc = SDB_OK ;
      rc = sdb_decimal_mod( &_decimal, &(right._decimal),
                            &(result._decimal) ) ;
      if ( SDB_OK != rc )
      {
         return rc ;
      }

      return sdb_decimal_update_typemod( &(result._decimal), -1 ) ;
   }

   INT32 bsonDecimal::updateTypemod( INT32 typemod )
   {
      return sdb_decimal_update_typemod( &_decimal, typemod ) ;
   }

   INT16 bsonDecimal::getWeight() const
   {
      return _decimal.weight ;
   }

   INT32 bsonDecimal::getTypemod() const
   {
      return _decimal.typemod ;
   }

   INT32 bsonDecimal::getPrecision( INT32 *precision, INT32 *scale ) const
   {
      return sdb_decimal_get_typemod( &_decimal, precision, scale ) ;
   }

   INT32 bsonDecimal::getPrecision() const 
   {
      INT32 rc        = SDB_OK ;
      INT32 precision = -1 ;
      INT32 scale     = -1 ;

      rc = sdb_decimal_get_typemod( &_decimal, &precision, &scale ) ;
      if ( SDB_OK != rc )
      {
         return -1 ;
      }

      return precision ;
   }

   // decimal->dscale | decimal->sign ;
   INT16 bsonDecimal::getStorageScale() const
   {
      return ( _decimal.dscale & SDB_DECIMAL_DSCALE_MASK ) | _decimal.sign ;
   }

   INT16 bsonDecimal::getScale() const
   {
      INT32 rc        = SDB_OK ;
      INT32 precision = -1 ;
      INT32 scale     = -1 ;

      rc = sdb_decimal_get_typemod( &_decimal, &precision, &scale ) ;
      if ( SDB_OK != rc )
      {
         return -1 ;
      }

      return scale ;
   }

   INT16 bsonDecimal::getSign() const
   {
      return _decimal.sign ;
   }

   INT32 bsonDecimal::getNdigit() const
   {
      return _decimal.ndigits ;
   }

   const INT16* bsonDecimal::getDigits() const
   {
      return _decimal.digits ;
   }

   INT32 bsonDecimal::getSize() const
   {
      return ( SDB_DECIMAL_HEADER_SIZE + _decimal.ndigits * sizeof( short ) ) ;
   }
}

