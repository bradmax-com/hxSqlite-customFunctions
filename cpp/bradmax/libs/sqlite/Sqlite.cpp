/*
 * Copyright (C)2005-2012 Haxe Foundation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

#include <hxcpp.h>
#include "sqlite3.h"
#include "Import.h"
#include <stdlib.h>
#include "bradmax/sys/db/UserFunction.h"
#include <sstream>
#include <iostream>
#include <memory>
#include <functional>


// Put in anon-namespace to avoid conflicts if static-linked
namespace {



struct result : public hx::Object
{
   HX_IS_INSTANCE_OF enum { _hx_ClassId = hx::clsIdSqlite };

   sqlite3 *db;
   sqlite3_stmt *r;
   int ncols;
   int count;
   String *names;
   int *bools;
   int done;
   int first;

   void create(sqlite3 *inDb, sqlite3_stmt *inR, String sql)
   {
      _hx_set_finalizer(this, finalize);

      db = inDb;
      r = inR;

      ncols = sqlite3_column_count(r);
      names = (String *)malloc(sizeof(String)*ncols);
      bools = (int*)malloc(sizeof(int)*ncols);
      first = 1;
      done = 0;
      for(int i=0;i<ncols;i++)
      {
         names[i] = String::makeConstString(sqlite3_column_name(r,i));
         for(int j=0;j<i;j++)
            if( names[j] == names[i] )
               hx::Throw(HX_CSTRING("Error, same field is two times in the request ") + sql);

         const char *dtype = sqlite3_column_decltype(r,i);
         bools[i] = dtype?(strcmp(dtype,"BOOL") == 0):0;
      }
   }

   static void finalize(Dynamic obj) { ((result *)(obj.mPtr))->destroy(false); }
   void destroy(bool inThrowError)
   {
      if (bools)
      {
         free(bools);
         bools = 0;
      }
      if (names)
      {
         free(names);
         names = 0;
      }
      if (r)
      {
         first = 0;
         done = 1;
         if( ncols == 0 )
            count = sqlite3_changes(db);

         bool err = sqlite3_finalize(r) != SQLITE_OK;
         db = 0;
         r = 0;

         if( err && inThrowError)
            hx::Throw(HX_CSTRING("Could not finalize request"));
      }
   }

   String toString() { return HX_CSTRING("Sqlite Result"); }

 //static void finalize_result( result *r, int exc, bool throwError = true )
};



/**
   <doc>
   <h1>SQLite</h1>
   <p>
   Sqlite is a small embeddable SQL database that store all its data into
   a single file. See http://sqlite.org for more details.
   </p>
   </doc>
**/

struct database : public hx::Object
{
   sqlite3 *db;
   hx::ObjectPtr<result> last;

   void create(sqlite3 *inDb)
   {
      db = inDb;
      _hx_set_finalizer(this, finalize);
   }
   static void finalize(Dynamic obj) { ((database *)(obj.mPtr))->destroy(false); }
   void destroy(bool inThrowError)
   {
      if (db)
      {
         if (last.mPtr)
         {
            last->destroy(inThrowError);
            last = null();
         }

         if( sqlite3_close(db) != SQLITE_OK )
         {
            if (inThrowError)
               hx::Throw(HX_CSTRING("Sqlite: could not close"));
         }
         db = 0;
      }
   }


   void setResult(result *inResult)
   {
      if (last.mPtr)
         last->destroy(true);

      last = inResult;
      HX_OBJ_WB_GET(this, last.mPtr);
   }

   void __Mark(hx::MarkContext *__inCtx) { HX_MARK_MEMBER(last); }
   #ifdef HXCPP_VISIT_ALLOCS
   void __Visit(hx::VisitContext *__inCtx) { HX_VISIT_MEMBER(last); }
   #endif

   String toString() { return HX_CSTRING("Sqlite Databse"); }
};

static void sqlite_error( sqlite3 *db ) {
   hx::Throw( HX_CSTRING("Sqlite error : ") + String(sqlite3_errmsg(db)) );
}

database *getDatabase(Dynamic handle)
{
   database *db = dynamic_cast<database *>(handle.mPtr);
   if (!db || !db->db)
      hx::Throw( HX_CSTRING("Invalid sqlite database") );
   return db;
}


result *getResult(Dynamic handle, bool inRequireStatement)
{
   result *r = dynamic_cast<result *>(handle.mPtr);
   if (!r || (inRequireStatement && !r->r))
      hx::Throw( HX_CSTRING("Invalid sqlite result") );
   return r;
}


} // End anon-namespace




/**
   connect : filename:string -> 'db
   <doc>Open or create the database stored in the specified file.</doc>
**/
Dynamic _bradmax_sqlite_connect(String filename)
{
   int err;
   sqlite3 *sqlDb = 0;
   if( (err = sqlite3_open(filename.__s,&sqlDb)) != SQLITE_OK )
      sqlite_error(sqlDb);

   database *db = new database();
   db->create(sqlDb);
   return db;
}


/**
   close : 'db -> void
   <doc>Closes the database.</doc>
**/
void _bradmax_sqlite_close(Dynamic handle)
{
   database *db = getDatabase(handle);
   db->destroy(true);
}

/**
   last_insert_id : 'db -> int
   <doc>Returns the last inserted auto_increment id.</doc>
**/
int     _bradmax_sqlite_last_insert_id(Dynamic handle)
{
   database *db = getDatabase(handle);
   return sqlite3_last_insert_rowid(db->db);
}

/**
   request : 'db -> sql:string -> 'result
   <doc>Executes the SQL request and returns its result</doc>
**/
Dynamic _bradmax_sqlite_request(Dynamic handle,String sql)
{
   database *db = getDatabase(handle);


   sqlite3_stmt *statement = 0;
   const char *tl = 0;
   if( sqlite3_prepare(db->db,sql.__s,sql.length,&statement,&tl) != SQLITE_OK )
   {
      hx::Throw( HX_CSTRING("Sqlite error in ") + sql + HX_CSTRING(" : ") +
                  String(sqlite3_errmsg(db->db) ) );
   }
   if( *tl )
   {
      sqlite3_finalize(statement);
      hx::Throw(HX_CSTRING("Cannot execute several SQL requests at the same time"));
   }

   int i,j;

   result *r = new result();
   r->create(db->db, statement,sql);

   db->setResult(r);

   return r;
}







/**
   result_get_length : 'result -> int
   <doc>Returns the number of rows in the result or the number of rows changed by the request.</doc>
**/
int  _bradmax_sqlite_result_get_length(Dynamic handle)
{
   result *r = getResult(handle,false);
   if( r->ncols != 0 )
      hx::Throw(HX_CSTRING("Getting change count from non-change request")); // ???
   return r->count;
}

/**
   result_get_nfields : 'result -> int
   <doc>Returns the number of fields in the result.</doc>
**/
int     _bradmax_sqlite_result_get_nfields(Dynamic handle)
{
   return getResult(handle,true)->ncols;
}

/**
   result_next : 'result -> object?
   <doc>Returns the next row in the result or [null] if no more result.</doc>
**/

Dynamic _bradmax_sqlite_result_next(Dynamic handle)
{
   result *r = getResult(handle,false);
   if( r->done )
      return null();

   switch( sqlite3_step(r->r) )
   {
      case SQLITE_ROW:
      {
         hx::Anon v = hx::Anon_obj::Create();
         r->first = 0;
         for(int i=0;i<r->ncols;i++)
         {
            Dynamic f;
            switch( sqlite3_column_type(r->r,i) )
            {
            case SQLITE_NULL:
               break;
            case SQLITE_INTEGER:
               if( r->bools[i] )
                  f = bool(sqlite3_column_int(r->r,i));
               else
                  f = int(sqlite3_column_int(r->r,i));
               break;
            case SQLITE_FLOAT:
               f = Float(sqlite3_column_double(r->r,i));
               break;
            case SQLITE_TEXT:
               f = String((char*)sqlite3_column_text(r->r,i));
               break;
            case SQLITE_BLOB:
               {
                  int size = sqlite3_column_bytes(r->r,i);
                  f = String((const char *)sqlite3_column_blob(r->r,i),size).dup();
                  break;
               }
            default:
               {
                  hx::Throw( HX_CSTRING("Unknown Sqlite type #") +
                               String((int)sqlite3_column_type(r->r,i)));
               }
            }
            v->__SetField(r->names[i],f,hx::paccDynamic);
         }
         return v;
      }
      case SQLITE_DONE:
         r->destroy(true);
         return null();
      case SQLITE_BUSY:
         hx::Throw(HX_CSTRING("Database is busy"));
      case SQLITE_ERROR:
         sqlite_error(r->db);
      default:
         hx::Throw(HX_CSTRING("Unkown sqlite result"));
   }

   return null();
}


static sqlite3_stmt *prepStatement(Dynamic handle,int n)
{
   result *r = getResult(handle,true);
   if( n < 0 || n >= r->ncols )
      hx::Throw( HX_CSTRING("Sqlite: Invalid index") );

   if( r->first )
      _bradmax_sqlite_result_next(handle);

   if( r->done )
      hx::Throw( HX_CSTRING("Sqlite: no more results") );

   return r->r;
}

/**
   result_get : 'result -> n:int -> string
   <doc>Return the [n]th field of the current result row.</doc>
**/


String  _bradmax_sqlite_result_get(Dynamic handle,int n)
{
   sqlite3_stmt *r = prepStatement(handle,n);
   return String((char*)sqlite3_column_text(r,n));
}

/**
   result_get_int : 'result -> n:int -> int
   <doc>Return the [n]th field of the current result row as an integer.</doc>
**/
int     _bradmax_sqlite_result_get_int(Dynamic handle,int n)
{
   sqlite3_stmt *r = prepStatement(handle,n);
   return sqlite3_column_int(r,n);
}

/**
   result_get_float : 'result -> n:int -> float
   <doc>Return the [n]th field of the current result row as a float.</doc>
**/
Float   _bradmax_sqlite_result_get_float(Dynamic handle,int n)
{
   sqlite3_stmt *r = prepStatement(handle,n);
   return sqlite3_column_double(r,n);
}






typedef struct
{
  String                func_name;     /* SQL function name */
  String                cb_name;       /* HAXE Callback name */
  Dynamic               ref;       /* HAXE Callback name */
  Dynamic               mem;       /* HAXE Callback name */
  int                   cb_num_args;   /* # arguments */
  int                   sum;   /* # arguments */
  int                   step;   /* # arguments */
} hx_sqlite3_func_t;

void _userAggregate(sqlite3_context *context, int argc, sqlite3_value **argv){
    SCtx *p=NULL;
    hx_sqlite3_func_t* func;
    int n;
    int f = 0;

    p = (SCtx *) sqlite3_aggregate_context(context, sizeof(*p));

    if( p->cnt == 0)
        p->sscnt++;

    p->cnt++;
    p->step++;

    func = (hx_sqlite3_func_t*) sqlite3_user_data(context);
    Array<Dynamic> fields = Array_obj<Dynamic>::__new();


    for (n=0; n < argc; n++)
    {
        switch (sqlite3_value_type (argv[n]))
        {
            case  SQLITE_INTEGER:
                fields->push(sqlite3_value_int(argv[n]));
                break;
            
            case SQLITE_FLOAT:
                fields->push(sqlite3_value_double(argv[n]));
                break;
            
            case SQLITE_BLOB:
                {
                    unsigned char* ptr = (unsigned char*)sqlite3_value_blob(argv[n]);
                    int blob_len = sqlite3_value_bytes(argv[n]);
                    Dynamic bytes = bradmax::sys::db::UserFunction_obj::processBytes(ptr, blob_len);
                    fields->push(bytes);
                }
                break;
            
            case SQLITE_TEXT:
                fields->push(sqlite3_value_text(argv[n]));
                break;
                
            case SQLITE_NULL:
            default:
                fields->push("");
            }
            f++;
    }

    int res = bradmax::sys::db::UserFunction_obj::acall(func->func_name, func->ref, fields, p);
}

void _userFunction(sqlite3_context *context, int argc, sqlite3_value **argv){
    hx_sqlite3_func_t* func;
    int n;
    func = (hx_sqlite3_func_t*) sqlite3_user_data(context);
    Array<Dynamic> fields = Array_obj<Dynamic>::__new();

    for (n=0; n < argc; n++)
    {

        switch (sqlite3_value_type (argv[n]))
        {
            case  SQLITE_INTEGER:
                fields->push(sqlite3_value_int(argv[n]));
                break;
            
            case SQLITE_FLOAT:
                fields->push(sqlite3_value_double(argv[n]));
                break;
            
            case SQLITE_TEXT:
                fields->push(sqlite3_value_text(argv[n]));
                break;
                
            case SQLITE_NULL:
            default:
                fields->push("NULL");
            }
    }

    Array<Dynamic> res = bradmax::sys::db::UserFunction_obj::call(func->func_name, func->ref, fields);
    int res_status = (int)res[0];
    int res_type = (int)res[1];
    Dynamic res_value = (Dynamic)res[2];

    func->step++;

    switch(res_type){
        case 0: //null
            sqlite3_result_null(context);
            break;
        case 1: //int
            sqlite3_result_int(context, (int)res_value);
            break;
        case 2: //double
            sqlite3_result_double(context, (float)res_value);
            break;
        case 3: //string
            sqlite3_result_text(context, ((String)res_value).__s, ((String)res_value).length, SQLITE_TRANSIENT);
            break;
        default: //error
            sqlite3_result_error(context, "invalid return type.", -1);
            break;
    }
}

void     _finalize(sqlite3_context *context){
    SCtx *p=NULL;
    hx_sqlite3_func_t* func;

    p = (SCtx *) sqlite3_aggregate_context(context, sizeof(*p));
    func = (hx_sqlite3_func_t*) sqlite3_user_data(context);    

    Array<Dynamic> res = bradmax::sys::db::UserFunction_obj::fcall(func->func_name, func->ref, p);

    int res_status = (int)res[0];
    int res_type = (int)res[1];
    Dynamic res_value = (Dynamic)res[2];

    switch(res_type){
        case 0: //null
            sqlite3_result_null(context);
            break;
        case 1: //int
            sqlite3_result_int(context, (int)res_value);
            break;
        case 2: //double
            sqlite3_result_double(context, (float)res_value);
            break;
        case 3: //string
            sqlite3_result_text(context, ((String)res_value).__s, ((String)res_value).length, SQLITE_TRANSIENT);
            break;
        default: //error
            sqlite3_result_error(context, "invalid return type.", -1);
            break;
    }


}

int _bradmax_sqlite_register_aggregate(Dynamic handle,String name, int n, Dynamic ref)
{
    database *db = getDatabase(handle);
  
    hx_sqlite3_func_t*   func;
    func = (hx_sqlite3_func_t*) malloc (sizeof(hx_sqlite3_func_t));

    func->func_name   = name;
    func->cb_name     = name;
    func->ref     = ref;
    func->cb_num_args = n;
    func->sum = 0;
    func->step = 0;

    return sqlite3_create_function(db->db, name.__s, n, SQLITE_UTF8, func, NULL, &_userAggregate, &_finalize);
    return 0;
}

int _bradmax_sqlite_register_function(Dynamic handle,String name, int n, Dynamic ref)
{
    database *db = getDatabase(handle);
  
    hx_sqlite3_func_t*   func;
    func = (hx_sqlite3_func_t*) malloc (sizeof(hx_sqlite3_func_t));

    func->func_name   = name;
    func->cb_name     = name;
    func->ref     = ref;
    func->cb_num_args = n;
    func->sum = 0;
    func->step = 0;

    return sqlite3_create_function(db->db, name.__s, n, SQLITE_UTF8, func, &_userFunction, NULL, NULL);
    return 0;
}