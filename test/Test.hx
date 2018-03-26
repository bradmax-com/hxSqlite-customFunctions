package;

import bradmax.sys.db.SqliteConnection;

class Test
{
    public static function main()
    {
        new Test();
    }

    public function new()
    {
        var s = new SqliteConnection("test.sqlite");
        s.registerFunction("test_max_2", 2, testMax2);
        s.registerFunction("test_max", -1, testMax);
        s.registerAggregate("test_avg", 1, testAvg, testAvgFin);

        var r = s.request("select test_max_2(1, 2)");

        while(r.hasNext())
            trace(r.next());

        var r = s.request("select test_max(7, 1, 2, 3, 4, 5, 6)");

        while(r.hasNext())
            trace(r.next());

        var r = s.request("drop table if exists kv");
        var r = s.request("create table kv(id INTEGER PRIMARY KEY AUTOINCREMENT, key text NOT NULL, value real)");
        var r = s.request("insert into kv (key, value) values('a', 0.0)");
        var r = s.request("insert into kv (key, value) values('b', 1.1)");
        var r = s.request("insert into kv (key, value) values('c', 2.2)");
        var r = s.request("select * from kv");
        var r = s.request("select test_avg(value) from kv");



            
        while(r.hasNext())
            trace(r.next());
    }

    function testAvg(params:Array<Dynamic>, data:Dynamic):Dynamic
    {
        if(data.value == null){
            data.value = 0.0;
            data.steps = 0;
        }

        data.steps++;
        var n = cast(params[0], Float);
        data.value += n;


        return 0;
    }

    function testAvgFin(data:Dynamic):Dynamic
    {
        return data.value / data.steps;
    }

    function testMax(params:Array<Dynamic>):Dynamic
    {
        var n:Float;
        var max:Float = cast(params[0], Float);

        for(i in 1...params.length){
            n = cast(params[i], Float);
            max = max > n ? max : n;
        }

        return max;
    }

    function testMax2(params:Array<Dynamic>):Dynamic
    {
        var a = cast(params[0], Float);
        var b = cast(params[1], Float);

        return a > b ? a : b;
    }
}