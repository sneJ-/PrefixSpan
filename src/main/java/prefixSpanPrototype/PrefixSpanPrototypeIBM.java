package prefixSpanPrototype;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class PrefixSpanPrototypeIBM {

	/**
	 * Testing the PrefixSpanPrototype with a data file from IBM data generator
	 * @param args input file, output file, threshold, verbose level
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		if(args.length == 4){
			
			List<int[]> data = new ArrayList<int[]>();
			//Read the data from the file
			BufferedReader br = new BufferedReader(new FileReader(args[0]));
			try{
				String line = br.readLine();
				while(line != null){
					String[] splitted = line.split(" ");
					int numberOfItems = Integer.parseInt(splitted[1]);
					int[] sequence = new int[splitted.length];
					sequence[0] = 0;
					for(int c = 1; c<sequence.length-1; c++){
						if(numberOfItems == 0){
							numberOfItems = Integer.parseInt(splitted[c+1])+1;
							sequence[c] = 0;
						}else{
							sequence[c] = Integer.parseInt(splitted[c+1]);
						}
						numberOfItems--;
					}
					sequence[sequence.length-1] = 0;
					data.add(sequence);
					line = br.readLine();
				}
			}finally{
				br.close();
			}
			
			//run algorithm
			long threshold = (long)Math.ceil(Double.parseDouble(args[2]) * data.size());
			System.out.println("Number of sequences: " +data.size());
			System.out.println("Threshold: " + threshold);
			PrefixSpanPrototype prefixSpan = new PrefixSpanPrototype(data, threshold, Integer.parseInt(args[3]));
			prefixSpan.run();
			Map<int[],Long> frequentPatterns = sortByComparator(prefixSpan.getFrequentPatterns());
			
			//file output
			PrintStream ps = new PrintStream(new FileOutputStream(args[1]));
			ps.println("Result for PrefixSpanPrototypeIBM.jar "+args[0]+ " "+args[1] + " " + args[2] + " " + args[3]);
			for(Map.Entry<int[], Long> cursor : frequentPatterns.entrySet()){
				for(int item : cursor.getKey()){
					ps.print(item+" ");
				}
				ps.println(" : " + cursor.getValue());
			}
			ps.close();
		}
		else{
			System.out.println("Error");
			System.out.println("PrefixSpanPrototypeIBM input file, output file, minSupport, verbose level");
		}
	}
	
	/**
	 * Sorts the Map according to its value DESC
	 * @param unsortedMap
	 * @return sortedMap
	 */
	private static Map<int[],Long> sortByComparator(Map<int[],Long> unsortedMap){
		List<Entry<int[],Long>> list = new LinkedList<Entry<int[],Long>>(unsortedMap.entrySet());
		
		Collections.sort(list, new Comparator<Entry<int[],Long>>(){
			@Override
			public int compare(Entry<int[], Long> arg0, Entry<int[], Long> arg1) {
				return arg1.getValue().compareTo(arg0.getValue());
			}
		});
		
		Map<int[],Long> sortedMap = new LinkedHashMap<int[],Long>();
		for(Entry<int[],Long> entry : list){
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}
}












