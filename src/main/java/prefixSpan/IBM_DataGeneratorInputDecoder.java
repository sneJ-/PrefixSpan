package prefixSpan;

import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;


/**
 * Input decoder for files generated by the IBM data generator.
 * 
 * @author Jens R�wekamp, Tianlong Du
 *
 */
public class IBM_DataGeneratorInputDecoder implements InputDecoder {

	@Override
	public DataSet<Tuple2<Long, int[]>> parse(String inputFile, ExecutionEnvironment env) throws Exception {
		DataSet<Tuple2<Long,int[]>> input = DataSetUtils.zipWithIndex(env.readTextFile(inputFile).map(new SequenceInputMapper())); //sets index
		List<Tuple2<Long, int[]>> inputList = input.collect(); //needed to avoid exceptions
		return env.fromCollection(inputList);
	}

	private class SequenceInputMapper implements MapFunction<String, int[]> {
		private static final long serialVersionUID = -3804203311043200652L;
		@Override
		public int[] map(String rawInput) throws Exception {
			String[] splitted = rawInput.split(" ");
			int numberOfItems = Integer.parseInt(splitted[1]);
			int[] sequence = new int[splitted.length];
			sequence[0] = 0;
			for (int c = 1; c < sequence.length - 1; c++) {
				if (numberOfItems == 0) {
					numberOfItems = Integer.parseInt(splitted[c + 1]) + 1;
					sequence[c] = 0;
				} else {
					sequence[c] = Integer.parseInt(splitted[c + 1]);
				}
				numberOfItems--;
			}
			sequence[sequence.length - 1] = 0;
			return sequence;
		}
	}
}
