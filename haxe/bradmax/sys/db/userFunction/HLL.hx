package bradmax.sys.db.userFunction;

import processing.structures.HyperLogLogPlusPlus;

typedef HllModel = {
    var bytes:haxe.io.Bytes;
}

class HLL
{ 
    public static function FUNCTION(args:Array<Dynamic>, data:HllModel):Int
    {
        var b:haxe.io.Bytes = null;
        b = cast(args[0], haxe.io.Bytes);

        if(b == null || b.length == 0)
            return 0;

        if(data.bytes == null){
            var copy = haxe.io.Bytes.alloc(b.length);
            for(i in 0...b.length){
                copy.set(i, b.get(i));
            }
                
            data.bytes = copy;
        }else{
            var bytes = data.bytes;
            for(i in 0...bytes.length){
                var bOld = bytes.get(i);
                var bNew = b.get(i);
                if(bNew > bOld)
                    bytes.set(i, bNew);
            }
        }
        
        return 0;
    }

    public static function FINALIZE(data:Dynamic)
    {
        var bytes = data.bytes;
        var hll = new HyperLogLogPlusPlus(14, bytes);
        return hll.count();
    }
}