package com.futuristGDF.kafkaexamples;

 
import java.io.UnsupportedEncodingException;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
 
public class ConsumerRunner implements Runnable {
    private KafkaStream m_stream;
    private int m_threadNumber;
 
    public ConsumerRunner(KafkaStream a_stream, int a_threadNumber) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
    }
 
    public void run() {
    	 long start = 0;
	 	 long delta = 0;
	 	 String s = null;
	 	
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
      
        while (it.hasNext()){
   
        	start = System.nanoTime();
        	
        	s = new String(it.next().message());
             
              //System.out.println("Thread " + m_threadNumber + ": " + new String(it.next().message()));
        
              //System.out.println("Thread " + m_threadNumber + ": " + s);

        	  delta= System.nanoTime() - start;
        	  int size = 0;
        	  
				try {
					size = s.getBytes("utf8").length;
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
        	 
        	  System.out.println("Thread : " + m_threadNumber + ":, duration: " + delta);
        	  
        	  s = null;
        	  start = 0;
        	  delta = 0;
        	 
        }
        
        System.out.println("Shutting down Thread: " + m_threadNumber);
    }
}

