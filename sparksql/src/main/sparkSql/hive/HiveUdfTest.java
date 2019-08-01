package sparkSql.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

public class HiveUdfTest extends UDF {
    public int evaluate(int x){
        return x*2;
    }
}
