package bradmax.sys.db;

import sys.db.ResultSet;

@:buildXml('
<set name="SQLITEDRIVER" value="${haxelib:hxsqlite_custom_functions}" />

<files id="haxe">
<flag value="-I${SQLITEDRIVER}" />

<compilerflag value="-Iinclude"/>
  <file name="${SQLITEDRIVER}/../cpp/bradmax/libs/sqlite/Sqlite.cpp" >
   <depend name="${SQLITEDRIVER}/../cpp/bradmax/libs/sqlite/Import.h"/>
   <depend name="${SQLITEDRIVER}/../cpp/bradmax/libs/sqlite/sqlite3.h"/>
  </file>

<compilerflag value="-Iinclude"/>
  <file name="${SQLITEDRIVER}/../cpp/bradmax/libs/sqlite/sqlite3.c" >
   <depend name="${SQLITEDRIVER}/../cpp/bradmax/libs/sqlite/sqlite3.h"/>
  </file>
</files>

<files id="haxe">
  <compilerflag value="-I${SQLITEDRIVER}" />
</files>

<files id="__main__">
  <compilerflag value="-I./include"/>
</files>
')

@:cppInclude('../cpp/bradmax/libs/sqlite/Import.h')

class SqliteConnection implements sys.db.Connection
{
    var c : Dynamic;
    var userFunctions = new Map<String, Dynamic>();
    var userAggregates = new Map<String, Dynamic>();
    var userFinalizers = new Map<String, Dynamic>();

    public function new( file : String ) {
        c = _connect(file);
    }

    public function close() {
        _close(c);
    }

    public function request( s : String ) : ResultSet {
        try {
            return new SqliteResultSet(_request(c,s)); 
        } catch( e : String ) {
            throw "Error while executing "+s+" ("+e+")";
        }
    }

    public function escape( s : String ) {
        return s.split("'").join("''");
    }

    public function quote( s : String ) {
        if( s.indexOf("\000") >= 0 )
        {
            var hexChars = new Array<String>();
            for(i in 0...s.length)
            hexChars.push( StringTools.hex( StringTools.fastCodeAt(s,i),2 ) );
            return "x'"+ hexChars.join("") +"'";
        }
        return "'"+s.split("'").join("''")+"'";
    }

    public function addValue( s : StringBuf, v : Dynamic ) {
        if (v == null) {
            s.add(v);
        }
        else if (Std.is(v,Bool)) {
                s.add( v ? 1 : 0 );
            } else {
            var t:Int = untyped v.__GetType();
            if( t == 0xff )
                s.add(v);
            else if( t == 2 )
                s.add( untyped v.__GetInt() );
            else
                s.add(quote(Std.string(v)));
        }
    }

    public function lastInsertId() : Int{
        return _last_id(c);
    }

    public function registerFunction(name:String, n:Int, func:Dynamic) : Int{
        userFunctions.set(name, func);
        return _register_function(c, name, n, this);
    }

    public function registerAggregate(name:String, n:Int, fun:Dynamic, fin:Dynamic) : Int{
        userAggregates.set(name, fun);
        userFinalizers.set(name, fin);
        return _register_aggregate(c, name, n, this);
    }

    public function dbName() {
        return "SQLite";
    }

    public function startTransaction() {
        request("BEGIN TRANSACTION");
    }

    public function commit() {
        request("COMMIT");
        startTransaction(); // match mysql usage
    }

    public function rollback() {
        request("ROLLBACK");
        startTransaction(); // match mysql usage
    }

    public function callFunction(name:String, args:Array<Dynamic>){
        var f:Dynamic = userFunctions.get(name);
        return f(args);
    }

    public function callAggregate(name:String, args:Array<Dynamic>, memory:Dynamic){
        var f:Dynamic = userAggregates.get(name);
        return f(args, memory);
    }

    public function callFinalizer(name:String, memory:Dynamic){
        var f:Dynamic = userFinalizers.get(name);
        return f(memory);
    }


    @:extern @:native("_bradmax_sqlite_connect")
    public static function _connect(filename:String):Dynamic return null;
    @:extern @:native("_bradmax_sqlite_request")
    public static function _request(handle:Dynamic,req:String):Dynamic return null;
    @:extern @:native("_bradmax_sqlite_close")
    public static function _close(handle:Dynamic):Void { };
    @:extern @:native("_bradmax_sqlite_last_insert_id")
    public static function _last_id(handle:Dynamic):Int return 0;
    @:extern @:native("_bradmax_sqlite_register_function")
    public static function _register_function(handle:Dynamic, name:String, n:Int, ref:Dynamic):Int return 0;
    @:extern @:native("_bradmax_sqlite_register_aggregate")
    public static function _register_aggregate(handle:Dynamic, name:String, n:Int, ref:Dynamic):Int return 0;
}



@:unreflective
@:structAccess
@:native("SCtx")
extern class StepContext {
    public var step:Int;
    public var cnt:Int;
    public var sscnt:Int;
    public var mem:Dynamic;
}

@:headerCode('
typedef struct SCtx SCtx;
    struct SCtx {
    int step;
    int cnt;
    int sscnt;
    Dynamic mem; 
};
')
class UserFunction
{
    public static function call(name:String, ref:Dynamic, args:Array<Dynamic>):Array<Dynamic>{
        for(i in 0...args.length){
            var r = args[i];
            if(r.b != null && Std.is(r.b, haxe.io.Bytes))
                args[i] = r.b;
        }

        var res = ref.callFunction(name, args);

        if(Std.is(res, null))
            return [0, 0, res];

        if(Std.is(res, Int))
            return [0, 1, res];

        if(Std.is(res, Float))
            return [0, 2, res];

        if(Std.is(res, String))
            return [0, 3, res];

        return [1, 10, null];
    }

    public static function acall(name:String, ref:Dynamic, args:Array<Dynamic>, memory:cpp.Star<StepContext>):Int{
        if(memory.mem == null)
            memory.mem = {};

        for(i in 0...args.length){
            var r = args[i];
            if(r.b != null && Std.is(r.b, haxe.io.Bytes))
                args[i] = r.b;
        }

        ref.callAggregate(name, args, memory.mem);
        return 0;
    }

    public static function fcall(name:String, ref:Dynamic, memory:cpp.Star<StepContext>):Array<Dynamic>{
        if(memory.mem == null)
            memory.mem = {};

        var res = ref.callFinalizer(name, memory.mem);

        if(Std.is(res, null))
            return [0, 0, res];

        if(Std.is(res, Int))
            return [0, 1, res];

        if(Std.is(res, Float))
            return [0, 2, res];

        if(Std.is(res, String))
            return [0, 3, res];

        if(res.b != null && Std.is(res.b, haxe.io.Bytes)){
            return [0, 4, res.b];
        }

        return [1, 10, null];
    }

    public static function processBytes(raw:cpp.Pointer<cpp.UInt8>, len:Int){
        var array = new Array<cpp.UInt8>();
        cpp.NativeArray.setUnmanagedData(array, raw, len);
        var bytes = haxe.io.Bytes.ofData(array);
        return {b:bytes};
    }
}



@:cppInclude('../cpp/bradmax/libs/sqlite/Import.h')
private class SqliteResultSet implements ResultSet {

    public var length(get,null) : Int;
    public var nfields(get,null) : Int;
    var r : Dynamic;
    var cache : List<Dynamic>;

    public function new( r:Dynamic ) {
        cache = new List();
        this.r = r;
        hasNext(); // execute the request
    }

    function get_length() {
        if( nfields != 0 ) {
            while( true ) {
                var c = result_next(r);
                if( c == null )
                    break;
                cache.add(c);
            }
            return cache.length;
        }
        return result_get_length(r);
    }

    function get_nfields() {
        return result_get_nfields(r);
    }

    public function hasNext() {
        var c = next();
        if( c == null )
            return false;
        cache.push(c);
        return true;
    }

    public function next() : Dynamic {
        var c = cache.pop();
        if( c != null )
            return c;
        return result_next(r);
    }

    public function results() : List<Dynamic> {
        var l = new List();
        while( true ) {
            var c = next();
            if( c == null )
                break;
            l.add(c);
        }
        return l;
    }

    public function getResult( n : Int ) {
        return new String(result_get(r,n));
    }

    public function getIntResult( n : Int ) : Int {
        return result_get_int(r,n);
    }

    public function getFloatResult( n : Int ) : Float {
        return result_get_float(r,n);
    }

    public function getFieldsNames() : Array<String> {
        return null;
    }



    @:extern @:native("_bradmax_sqlite_result_next")
    public static function result_next(handle:Dynamic):Dynamic return null;
    @:extern @:native("_bradmax_sqlite_result_get_length")
    public static function result_get_length(handle:Dynamic):Int return 0;
    @:extern @:native("_bradmax_sqlite_result_get_nfields")
    public static function result_get_nfields(handle:Dynamic):Int return 0;
    @:extern @:native("_bradmax_sqlite_result_get")
    public static function result_get(handle:Dynamic,i:Int) : String return null;
    @:extern @:native("_bradmax_sqlite_result_get_int")
    public static function result_get_int(handle:Dynamic,i:Int) : Int return 0;
    @:extern @:native("_bradmax_sqlite_result_get_float")
    public static function result_get_float(handle:Dynamic,i:Int):Float return 0.0;

}