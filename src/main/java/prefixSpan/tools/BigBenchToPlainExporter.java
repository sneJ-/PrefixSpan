package prefixSpan.tools;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import prefixSpan.BigBenchInputDecoder;
import prefixSpan.InputDecoder;

/**
 * Exports files from BigBench format to plainInputFormat.
 * @author Jens Röwekamp
 *
 */
public class BigBenchToPlainExporter {

	public static void main(String[] args) throws Exception {
		if(args.length == 2){
			final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			InputDecoder decoder = new BigBenchInputDecoder();
			decoder.parse(args[0], env).map(new MapFunction<Tuple2<Long,int[]>, Tuple2<Long,String>>(){
				private static final long serialVersionUID = -3979175244135608324L;
				@Override
				public Tuple2<Long, String> map(Tuple2<Long, int[]> arg0) throws Exception {
					StringBuilder builder = new StringBuilder();
					for(Integer i : arg0.f1){
						builder.append(i+",");
					}
					builder.deleteCharAt(builder.length()-1);
					return new Tuple2<Long,String>(arg0.f0,builder.toString());
				}
			}).sortPartition(0, Order.ASCENDING).writeAsCsv(args[1]).setParallelism(1);
			env.execute();
		}
		else{
			System.err.println("ERROR");
			System.err.println("BigBenchToPlainExporter inputFile outputFile");
		}
	}
}
