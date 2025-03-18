//https://raw.githubusercontent.com/jrderek/Big_Data_Engineering_Portfolio/bf7a5efb24f2c6e860e5ead544dadc08f791814e/Become%20a%20Data%20Engineer%20-%20Mastering%20the%20Concepts/4.%20Apache%20Spark%20Essential%20Training%20-%20Big%20Data%20Engineering/Java%20files/CustomAccuMap.java

package com.lynda.course.sparkbde;


import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.util.AccumulatorV2;

public class CustomAccuMap 
	extends AccumulatorV2<Map<String,Double>, Map<String,Double>> {
	 
    private Map<String,Double> myMap = 
    					new HashMap<String,Double>();
 
    public CustomAccuMap() {
        this(new HashMap<String,Double>());
    }
 
    public CustomAccuMap(Map<String,Double> initialValue) {
        if (initialValue != null) {
        	myMap = initialValue ;
        }
    }
 
    public void add(Map<String,Double> newMap) {
        
    	Iterator<String> dIterator = 
    					newMap.keySet().iterator();
    	while(dIterator.hasNext()) {
    		String key = dIterator.next();
    		if ( myMap.containsKey(key)) {
    			myMap.put(key, myMap.get(key) + 
    					newMap.get(key));
    		}
    		else {
    			myMap.put(key, newMap.get(key));
    		}
    	}
    	
    }
 
    public CustomAccuMap copy() {
        return (new CustomAccuMap(value()));
    }
 
    public boolean isZero() {
        return (myMap.size() == 0);
    }
 
    public void merge(AccumulatorV2<Map<String,Double>,
    			Map<String,Double>> other) {
        add(other.value());
    }
 
    public void reset() {
        myMap.clear();
    }
 
    public Map<String,Double> value() {
        return myMap;
    }


}