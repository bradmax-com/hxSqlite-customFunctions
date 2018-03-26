package;

import bradmax.sys.db.userFunction.HLL;
import bradmax.sys.db.SqliteConnection;

class Main
{
    public static function main(){
        new Main();

        for(i in 0...10){
            trace(i);
        }
    }

    public function new(){
        var s = new SqliteConnection("2.sqlite");
        s.registerAggregate("hll", 1, HLL.FUNCTION, HLL.FINALIZE);

        var r = s.request("select hll(users_hll) from account_users");

        while(r.hasNext())
            trace(r.next());
    }
}

