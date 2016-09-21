package prefixSpan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Flink implementation of the PrefixSpan algorithm.
 * @author Jens Röwekamp, Tianlong Du
 *
 */
public class PrefixSpan {

	/**
	 * Main Function to execute the algorithm from command line.
	 * Interprets the command line arguments and executes the algorithm.
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		if(args.length != 3){
			System.err.println("Error - Arguments");
			System.err.println("PrefixSpan.java inputFile decoder minSupport");
			System.err.println("decoder types: plain (PlainInputDecoder), bigBench (BigBenchInputDecoder), ibm (IBM_DataGeneratorInputDecoder) or dummy (DummyInputDecoder)");
		} else if(Double.parseDouble(args[2]) > 1 || Double.parseDouble(args[2]) < 0){
			System.err.println("Error - minSupport");
			System.err.println("PrefixSpan.java inputFile decoder minSupport");
			System.err.println("minSupport needs to be between 0 and 1");
		} else{
			InputDecoder decoder = null;
			switch(args[1]){
			case "plain": decoder = new PlainInputDecoder(); break;
			case "bigBench": decoder = new BigBenchInputDecoder(); break;
			case "ibm": decoder = new IBM_DataGeneratorInputDecoder(); break;
			case "dummy": decoder = new DummyInputDecoder(); break;
			default: System.err.println("Error - decoder");
				System.err.println("PrefixSpan.java inputFile decoder minSupport"); 
				System.err.println("decoder types: plain (PlainInputDecoder), bigBench (BigBenchInputDecoder), ibm (IBM_DataGeneratorInputDecoder) or dummy (DummyInputDecoder)");
			}
			if(decoder != null){
				PrefixSpan prefixSpan;
				if(decoder instanceof DummyInputDecoder){
					prefixSpan = new PrefixSpan(args[0],decoder,Double.parseDouble(args[2]));
				}else{
					prefixSpan = new PrefixSpan(args[0],decoder,Double.parseDouble(args[2]));
				}
				prefixSpan.run();
			}
		}
	}

	private final ExecutionEnvironment env;
	private long threshold;
	private long totalNumberOfSequences;
	private String inputFile;
	private DataSet<Tuple2<Long,int[]>> database;
	private InputDecoder decoder;
	
	/**
	 * Constructor.
	 * @param inputFile
	 * @param minSupport as percentage from total number of sequences between 0 and 1
	 * @throws Exception 
	 */
	public PrefixSpan(String inputFile, InputDecoder decoder, double minSupport) throws Exception{
		this.inputFile = inputFile;
		this.decoder = decoder;
		env = ExecutionEnvironment.getExecutionEnvironment();
		database = decoder.parse(inputFile, env);
		totalNumberOfSequences = database.count();
		System.out.println("Total number of sequences: " + totalNumberOfSequences);
		threshold = (long)Math.ceil(minSupport * totalNumberOfSequences);
		System.out.println("Threshold: " + threshold);
	}
	
	/**
	 * Executes the prefixSpan algorithm.
	 * First finds the frequent length 1 patterns in the whole database and then
	 * recursively checks for frequent sub patterns.
	 * @throws Exception
	 */
	public void run() throws Exception{
		//Step 1 find all frequent items in the whole database
		DataSet<Tuple2<Integer,Long>> frequentBasisItems = findFrequentLength1ItemsFirstRun();
		List<Tuple2<Integer,Long>> frequentBasisItemsList = frequentBasisItems.collect();
		database = database.map(new FilterInfrequentBaseItems(frequentBasisItemsList));
		if(frequentBasisItemsList.size() > 0){ //only if frequent basis items have been found.
			DataSet<Tuple2<String,Long>> frequentPatterns = env.fromCollection(frequentBasisItemsList).map(new FrequentBasisItemsMapper());
		
			//Step 2 recursively find all sub patterns
			for(Tuple2<Integer,Long> frequentBasisItem : frequentBasisItemsList){
				List<Tuple2<String,Long>> frequentPatternsFromBasisItemList = recursivelyFindFrequentPatterns(new int[]{frequentBasisItem.f0},null);
				if(!frequentPatternsFromBasisItemList.isEmpty()){
					DataSet<Tuple2<String,Long>> frequentPatternsFromBasisItem = env.fromCollection(frequentPatternsFromBasisItemList).map(new FrequentItemsMapper());
					frequentPatterns = frequentPatterns.union(frequentPatternsFromBasisItem); //TODO union bug
				}
			}
		
			//Step 3 result output
			if(decoder instanceof DummyInputDecoder){
				System.out.println("Total frequent patterns");
				frequentPatterns.sortPartition(1, Order.DESCENDING).setParallelism(1).print();
			}else{
				List<Tuple2<String,Long>> finalFrequentPatternsList = frequentPatterns.collect();
				env.fromCollection(finalFrequentPatternsList).sortPartition(1, Order.DESCENDING).setParallelism(1).writeAsCsv(inputFile+".result.csv");
				env.execute();
				System.out.println("Result written to " + inputFile+".result.csv");
			}
		}else{ //No frequent patterns have been found.
				System.out.println("No frequent patterns have been found.");
		}
	}
	
	/**
	 * Recursive function which finds all frequent patterns and builds the frequent sub patterns.
	 * @param oldPrefix to build newPrefix
	 * @param prefixPointer to begin building the newPrefix
	 * @return DataSet of frequent sub patterns and their frequency
	 * @throws Exception 
	 */
	private List<Tuple2<String,Long>> recursivelyFindFrequentPatterns(int[] oldPrefix, Map<Long,Integer> prefixPointer) throws Exception{
		List<Tuple2<String,Long>> frequentPatterns = new ArrayList<Tuple2<String,Long>>();
		int[] relevantItemSetPrefix = splitPrefix(oldPrefix);
		prefixPointer = projectDatabase(relevantItemSetPrefix,prefixPointer);
		DataSet<Tuple2<Integer,Long>> prefixPatterns = findFrequentLength1Items(prefixPointer);
		List<Tuple2<Integer,Long>> prefixPatternList = prefixPatterns.collect();
		
		//builds the new frequent sub patterns from oldPrefix and prefixPatternList
		for(Tuple2<Integer,Long> prefixPattern : prefixPatternList){
			//build the new prefix
			StringBuilder frequentPattern = new StringBuilder();
			int[] newPrefix;
			if(prefixPattern.f0 < 0){ //inside itemset
				newPrefix = new int[oldPrefix.length+1];
				newPrefix[oldPrefix.length] = -prefixPattern.f0;
			}else{ //outside itemset
				newPrefix = new int[oldPrefix.length+2];
				newPrefix[oldPrefix.length] = 0;
				newPrefix[oldPrefix.length+1] = prefixPattern.f0;
			}
			
			for(int i=0; i<oldPrefix.length; i++){
				newPrefix[i] = oldPrefix[i];
			}
			
			//build the output string
			for(int item : newPrefix){
				if(item == 0){
					frequentPattern.append("|");
				}else {
					frequentPattern.append(" " + item + " " );
				}
			}
			
			frequentPatterns.add(new Tuple2<String,Long>(frequentPattern.toString(), prefixPattern.f1));
			
			//clone the prefixPointer
			Map<Long,Integer> prefixPointerClone = new HashMap<Long,Integer>();
			prefixPointerClone.putAll(prefixPointer);
			
			//recursively calls itself until no frequent sub patterns are found
			List<Tuple2<String,Long>>collectedResults = recursivelyFindFrequentPatterns(newPrefix, prefixPointerClone);
			frequentPatterns.addAll(collectedResults);
		}
		return frequentPatterns;
	}
	
	/**
	 * Projects the database by setting new pointers.
	 * @param relevantItemSetPrefix relevant prefix to project database
	 * @param prefixPointerList  prefix pointer from where to start projecting
	 * @return new pointers of the projected database
	 * @throws Exception 
	 */
	private Map<Long,Integer> projectDatabase(int[] relevantItemSetPrefix, Map<Long,Integer> prefixPointer) throws Exception {
		Map<Long,Integer> newPrefixPointer = new HashMap<Long,Integer>();
		DataSet<Tuple2<Long,Integer>> prefixPointerDataSet = database.map(new PrefixPointerMapper(relevantItemSetPrefix,prefixPointer));
		List<Tuple2<Long,Integer>> prefixPointerList = prefixPointerDataSet.collect();
		for(Tuple2<Long,Integer> pointer : prefixPointerList){
			newPrefixPointer.put(pointer.f0, pointer.f1);
		}
		return newPrefixPointer;
	}
	
	/**
	 * Splits the old prefix at the last 0.
	 * @param oldPrefix which should be split
	 * @return itemset of the relevant prefix
	 */
	private int[] splitPrefix(int[] oldPrefix) {
		int last0pointer = 0;
		for(int i = oldPrefix.length-1; i>=0; i--){
			if(oldPrefix[i] == 0){
				last0pointer = i+1;
				break;
			}
		}
		int[] splittedPrefix = new int[oldPrefix.length-last0pointer];
		for(int i = 0; i<splittedPrefix.length; i++){
			splittedPrefix[i] = oldPrefix[last0pointer+i];
		}
		return splittedPrefix;
	}
	
	/**
	 * Finds all frequent length 1 items in the whole database.
	 * It checks if an item occurs in a sequence and sums the occurrences up.
	 * Afterwards it filters those items below the threshold.
	 */
	private DataSet<Tuple2<Integer,Long>> findFrequentLength1ItemsFirstRun(){
		return database.flatMap(new SequenceItemsFlatMapperFirstRun())
				.groupBy(0)
				.sum(1)
				.filter(new ThresholdFilter(threshold));
	}
	
	/**
	 * Finds the length 1 frequent sequential patterns.
	 * @param prefixPointerMap of the database to check from
	 * @return List of Tuple2 <frequent length 1 pattern, frequency>
	 */
	private DataSet<Tuple2<Integer, Long>> findFrequentLength1Items(Map<Long,Integer> prefixPointerMap) {
		DataSet<Tuple2<Integer, Long>> frequentItems = database.flatMap(new SequenceItemsFlatMapper(prefixPointerMap))
				.groupBy(0)
				.sum(1)
				.filter(new ThresholdFilter(threshold));
		return frequentItems;
	}

	/**
	 * Filters infrequent items from the database to reduce the amount of projections.
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class FilterInfrequentBaseItems implements MapFunction<Tuple2<Long,int[]>, Tuple2<Long,int[]>>{
		private static final long serialVersionUID = -4501554211461932704L;
		private Set<Integer> frequentBaseItems;
		public FilterInfrequentBaseItems(List<Tuple2<Integer,Long>> frequentBasisItemsList){
			Set<Integer> frequentBaseItems = new HashSet<Integer>();
			for(Tuple2<Integer,Long> seq : frequentBasisItemsList){
				frequentBaseItems.add(seq.f0);
			}
			this.frequentBaseItems = frequentBaseItems;
		}
		@Override
		public Tuple2<Long, int[]> map(Tuple2<Long, int[]> db) throws Exception {
			ArrayList<Integer> newSequence = new ArrayList<Integer>();
			for(Integer item : db.f1){
				if(frequentBaseItems.contains(item) || item == 0) newSequence.add(item);
			}
			int[] seq = new int[newSequence.size()];
			for(int i=0; i<seq.length; i++){
				seq[i] = newSequence.get(i);
			}
			return new Tuple2<Long, int[]>(db.f0, seq);
		}
	}
	
	/**
	 * Maps all single item occurrences of one sequence.
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class SequenceItemsFlatMapperFirstRun implements FlatMapFunction<Tuple2<Long,int[]>, Tuple2<Integer,Long>> {
		private static final long serialVersionUID = 8139091712335435056L;
		@Override
		public void flatMap(Tuple2<Long,int[]> sequence, Collector<Tuple2<Integer, Long>> out) throws Exception {
			//container for unique items
			Set<Integer> uniqueItems = new HashSet<Integer>();
			//add every item of sequence to set
			for(Integer item : sequence.f1){
				uniqueItems.add(item);
			}
			//construct the result
			for(Integer uniqueItem : uniqueItems){
				//no delimiting 0 shall be counted
				if(uniqueItem != 0)	out.collect(new Tuple2<Integer,Long>(uniqueItem,1L));
			}
		}
	}
	
	/**
	 * Maps all single item occurrences of one sequence.
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class SequenceItemsFlatMapper implements FlatMapFunction<Tuple2<Long,int[]>, Tuple2<Integer,Long>> {
		private static final long serialVersionUID = 9139091712335435056L;
		private Map<Long,Integer> prefixPointerMap;
		public SequenceItemsFlatMapper(Map<Long,Integer> prefixPointerMap){
			this.prefixPointerMap = prefixPointerMap;
		}
		@Override
		public void flatMap(Tuple2<Long,int[]> sequence, Collector<Tuple2<Integer, Long>> out) throws Exception {
			//If pointer isn't run out (-1)
			if(prefixPointerMap.get(sequence.f0) > 0){
				//container for unique items
				Set<Integer> uniqueItems = new HashSet<Integer>();
				Integer prefix = sequence.f1[prefixPointerMap.get(sequence.f0)-1];
				
				//Check if we are in an itemset or not
				boolean inItemSet = true;
				if(sequence.f1[prefixPointerMap.get(sequence.f0)] == 0){
					inItemSet = false;
				}
			
				//add every item of sequence to set, beginning from pointer
				for(int i = prefixPointerMap.get(sequence.f0); i < sequence.f1.length; i++){
					if(inItemSet){
						uniqueItems.add(-sequence.f1[i]);
						if(sequence.f1[i] == 0) inItemSet = false;
					}else{
						uniqueItems.add(sequence.f1[i]);
						if(sequence.f1[i] == prefix && i < sequence.f1.length-1){
							//add items which directly follow a found prefix as _item to uniqueItems
							for(int j=i+1; j < sequence.f1.length; j++){
								if(sequence.f1[j] == 0) break;
								uniqueItems.add(-sequence.f1[j]);
							}
						}
					}
				}
				//construct the result
				for(Integer uniqueItem : uniqueItems){
					//no delimiting 0 shall be counted
					if(uniqueItem != 0)	out.collect(new Tuple2<Integer,Long>(uniqueItem,1L));
				}
			}
		}
	}
	
	/**
	 * Projects the database by setting new pointers.
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class PrefixPointerMapper implements MapFunction<Tuple2<Long,int[]>,Tuple2<Long,Integer>>{
		private int[] prefix;
		private Map<Long,Integer> oldPrefixPointer;
		public PrefixPointerMapper(int[] relevantItemSetPrefix, Map<Long,Integer> oldPrefixPointer){
			this.prefix = relevantItemSetPrefix;
			this.oldPrefixPointer = oldPrefixPointer;
		}
		private static final long serialVersionUID = 7268987024695981745L;
		@Override
		public Tuple2<Long, Integer> map(Tuple2<Long, int[]> sequence) throws Exception {
			Long sequenceId = sequence.f0;
			Integer pointer;
			if(oldPrefixPointer == null){ //first run
				pointer = 0;
			}else{ //nth run
				pointer = oldPrefixPointer.get(sequenceId);
			}
			if(pointer>=0){ //only change the pointer if sequence wasn't finished already (pointer has value -1)
				
				//prefix is inside itemset, we've to project on the first occurrence of the whole pattern
				if(prefix.length>1){
					boolean prefixFound = false;
					boolean[] itemsFound = new boolean[prefix.length];
					for(int i=0; i<itemsFound.length; i++){
						itemsFound[i] = false;
					}
					//set the pointer backwards to determine the whole pattern
					if(pointer - (prefix.length - 1) >= 0) pointer -= (prefix.length-1);
					else pointer = 0;
					//find the first prefix occurrence
					for(int c=pointer; c<sequence.f1.length; c++){
						//check for the prefix
						for(int i=0; i<prefix.length; i++){
							if((int)sequence.f1[c] == (int)prefix[i]){
								itemsFound[i]=true;
								int counter = 0;
								for(int d=0; d<itemsFound.length; d++){
									if(itemsFound[d])counter++;
								}
								if(counter == prefix.length){
									prefixFound = true;
									pointer = c+1; //set pointer to next item
									break; //break out of for loop
								}
							}
							//if sequence[c] = 0 reset the itemsFound array to false
							if(sequence.f1[c] == 0){
								for(int d=0; d<itemsFound.length; d++){
									itemsFound[d] = false;
								}
							}
						}
					}
					if(!prefixFound){ //if prefix couldn't be found, set pointer to -1 to indicate termination
						pointer = -1;
					}
				}
				
				//prefix is outside itemset, we have to split on the first occurrence of the item
				else{ 
					boolean prefixFound = false;
					//find next delimiter
					for(int c = pointer; c<sequence.f1.length; c++){
						if(sequence.f1[c] == 0){
							pointer = c+1;
							break;
						}
					}
					//determine position of prefix
					for(int c = pointer; c<sequence.f1.length; c++){
						if((int)sequence.f1[c] == (int)prefix[0]){
							pointer = c+1; //set pointer to next item
							prefixFound = true;
							break;
						}
					}
					if(!prefixFound){ //if prefix couldn't be found, set pointer to -1 to indicate termination
						pointer = -1;
					}
				}
			}
			return new Tuple2<Long,Integer>(sequenceId,pointer);
		}
	}
	
	/**
	 * Filters all items with less occurrences than threshold.
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class ThresholdFilter implements FilterFunction<Tuple2<Integer,Long>>{
		private static final long serialVersionUID = 3911377333329109243L;
		long threshold;
		public ThresholdFilter(long threshold){
			this.threshold = threshold;
		}
		@Override
		public boolean filter(Tuple2<Integer, Long> item) throws Exception {
			return item.f1 >= threshold;
		}
	}
	
	/**
	 * Creates the first frequent patterns (for output) based on the frequent basis items.
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class FrequentBasisItemsMapper implements MapFunction<Tuple2<Integer,Long>, Tuple2<String,Long>>{
		private static final long serialVersionUID = 1882551481736413095L;
		@Override
		public Tuple2<String, Long> map(Tuple2<Integer, Long> in) throws Exception {
			return new Tuple2<String, Long>("<| " + in.f0 + " |>",in.f1);
		}
	}
	
	/**
	 * Creates the frequent sub patterns for output.
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class FrequentItemsMapper implements MapFunction<Tuple2<String,Long>, Tuple2<String,Long>>{
		private static final long serialVersionUID = 1882551481736413095L;
		@Override
		public Tuple2<String, Long> map(Tuple2<String, Long> in) throws Exception {
			return new Tuple2<String, Long>("<|" + in.f0 + "|>",in.f1);
		}
	}
}
