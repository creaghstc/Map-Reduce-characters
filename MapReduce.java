import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


//Conor Creagh 13454222
//Distributed Systems Assignment 2 

public class MapReduce {
        
        public static void main(String[] args) throws IOException, InterruptedException {
                
            	ExecutorService executor = Executors.newFixedThreadPool(Integer.valueOf(args[0]));//creating a pool of first argument threads  
                Map<String, String> input = new HashMap<String, String>();
				
                 for(int iterator = 1; iterator < args.length; iterator++){ //loop over arguments after pool size
                	InputStream instr = new FileInputStream(args[iterator]); //create input stream
                    BufferedReader buf = new BufferedReader(new InputStreamReader(instr)); //reader
	                String line = buf.readLine();
	                StringBuilder build = new StringBuilder(); 
	                
	                while(line != null){ //create a string for each file
	                	build.append(line).append(" ");
	                	line = buf.readLine();
	                }
					input.put(args[iterator],build.toString()); //add the file as string to input with file name as key
                }
        
                // APPROACH #3: Distributed MapReduce
                {
                        final Map<Character, Map<String, Integer>> output = new HashMap<Character, Map<String, Integer>>();
                        
                        // MAP:
                        
                        final List<MappedItem> mappedItems = new LinkedList<MappedItem>();
                        
                        final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
                                @Override
								public synchronized void mapDone(String file, List<MappedItem> results) {
								mappedItems.addAll(results);
								}
						};
                        
                        List<Thread> mapCluster = new ArrayList<Thread>(input.size());
                        
                        Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
                        while(inputIter.hasNext()) {
                                Map.Entry<String, String> entry = inputIter.next();
                                final String file = entry.getKey();
                                final String contents = entry.getValue();
                                
                                Thread t = new Thread(new Runnable() {
                                        @Override
                                        public void run() {
                                        	map(file, contents, mapCallback);
                                        }
                                });
                                mapCluster.add(t);
                                t.start();
                        }
                        
                        // wait for mapping phase to be over:
                        for(Thread t : mapCluster) {
                                try {
                                        t.join();
                                } catch(InterruptedException e) {
                                        throw new RuntimeException(e);
                                }
                        }
                        
                        // GROUP:
                        
                        Map<Character, List<String>> groupedItems = new HashMap<Character, List<String>>();
                        
                        Iterator<MappedItem> mappedIter = mappedItems.iterator();
                        while(mappedIter.hasNext()) {
                                MappedItem item = mappedIter.next();
                                char letter = item.getLetter();
                                String file = item.getFile();
                                List<String> list = groupedItems.get(letter);
                                if (list == null) {
                                        list = new LinkedList<String>();
                                        groupedItems.put(letter, list);
                                }
                                list.add(file);
                        }
                        
                        // REDUCE:
                        
                        final ReduceCallback<Character, String, Integer> reduceCallback = new ReduceCallback<Character, String, Integer>() {
                                public synchronized void reduceDone(Character k, Map<String, Integer> v) {
								output.put(k, v);
							}	

						
							
                        };
                        
                        List<Thread> reduceCluster = new ArrayList<Thread>(groupedItems.size());
                        
                        Iterator<Entry<Character, List<String>>> groupedIter = groupedItems.entrySet().iterator();
                        while(groupedIter.hasNext()) {
                                Entry<Character, List<String>> entry = groupedIter.next();
                                final char letter = entry.getKey();
                                final List<String> list = entry.getValue();
                                
                                Thread t = new Thread(new Runnable() {
                                        @Override
                                        public void run() {
                                                reduce(letter, list, reduceCallback);
                                        }
                                });
                                executor.execute(t); //executor execute thread
                        }
                        executor.shutdown(); //shutdown executor (thread pool)
                    
						executor.awaitTermination(24L, TimeUnit.HOURS); //equivalent of t.join();
				
                        System.out.println(output);
                        PrintWriter writer = new PrintWriter("results.txt", "UTF-8"); //create writer and file 
                        writer.println(output); //write results to a file 
						writer.close(); //close writer
                }
        }
        
        public static void map(String file, String contents, List<MappedItem> mappedItems) {
                String[] words = contents.trim().split("\\s+");
                for(String word: words) {
                        mappedItems.add(new MappedItem(word.charAt(0), file)); //take first letter of word
                }
        }
        
        public static void reduce(Character letter, List<String> list, HashMap<Character, Map<String, Integer>> output) {
                Map<String, Integer> reducedList = new HashMap<String, Integer>();
                for(String file: list) {
                        Integer occurrences = reducedList.get(file);
                        if (occurrences == null) {
                                reducedList.put(file, 1);
                        } else {
                                reducedList.put(file, occurrences.intValue() + 1);
                        }
                }
                output.put(letter, reducedList);
        }
        
        public static interface MapCallback<E, V> {
                
                public void mapDone(E key, List<V> values);
        }
        
        public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
                String[] words = contents.trim().split("\\s+");
                List<MappedItem> results = new ArrayList<MappedItem>(words.length);
                for(String word: words) {
                        results.add(new MappedItem(word.charAt(0), file));
                }
                callback.mapDone(file, results);
        }
        
        public static interface ReduceCallback<E, K, V> {
                
                public void reduceDone(E e, Map<K,V> results);

        }
        
        public static void reduce(char letter, List<String> list, ReduceCallback<Character, String, Integer> callback) {
                
                Map<String, Integer> reducedList = new HashMap<String, Integer>();
                for(String file: list) {
                        Integer occurrences = reducedList.get(file);
                        if (occurrences == null) {
                                reducedList.put(file, 1);
                        } else {
                                reducedList.put(file, occurrences.intValue() + 1);
                        }
                }
                callback.reduceDone(letter, reducedList);
        }
        
        private static class MappedItem { 
                
                private final char letter;
                private final String file;
                
                public MappedItem(char c, String file) {
                        this.letter = c;
                        this.file = file;
                }

                public char getLetter() {
                        return letter;
                }

                public String getFile() {
                        return file;
                }
                
                @Override
                public String toString() {
                        return "[\"" + letter + "\",\"" + file + "\"]";
                }
        }
}
