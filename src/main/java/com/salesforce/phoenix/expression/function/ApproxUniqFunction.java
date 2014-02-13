package com.salesforce.phoenix.expression.function;

import java.util.List;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.aggregator.Aggregator;
import com.salesforce.phoenix.expression.aggregator.BaseAggregator;
import com.salesforce.phoenix.parse.FunctionParseNode.Argument;
import com.salesforce.phoenix.parse.FunctionParseNode.BuiltInFunction;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.twitter.algebird.HLL;
import com.twitter.algebird.HyperLogLog;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 * @author Xi Liu
 */
@BuiltInFunction(name= ApproxUniqFunction.NAME,  args={ @Argument(allowedTypes={PDataType.VARBINARY})} )
public class ApproxUniqFunction extends SingleAggregateFunction {
  static final String NAME = "APPROX_UNIQ";

  public ApproxUniqFunction() {

  }

  public ApproxUniqFunction(List<Expression> childern) {
    super(childern);
  }

  @Override
  public Aggregator newServerAggregator() {
    return new BaseAggregator(null) {

      private HLL aggHLL = null;

      @Override
      public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {
        byte[] buffer = new byte[ptr.getLength()];
        System.arraycopy(ptr.get(), ptr.getOffset(), buffer, 0, ptr.getLength());
        HLL thisHll = HyperLogLog.fromBytes(buffer);
        if (aggHLL == null) {
          aggHLL = thisHll;
        } else {
          int aggBits = aggHLL.bits();
          int thisBits = thisHll.bits();
          if (thisBits == aggBits) {
            aggHLL = thisHll.$plus(aggHLL);
          } else if (thisBits > aggBits) {
            aggHLL = thisHll;
          }
        }

      }

      @Override
      public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        ptr.set(HyperLogLog.toBytes(aggHLL));
        return true;
      }

      @Override
      public PDataType getDataType() {
        return PDataType.VARBINARY;
      }
    };
  }

  @Override
  public String getName() {
    return NAME;
  }
}
