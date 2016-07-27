package arbitaryutils;

/*
 * 
 * 01. Generate random string, use org.apache.commons.lang3.RandomStringUtils
 */

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import org.apache.commons.lang3.RandomStringUtils;

public class SyntheticData {
	
	private static final int LIMIT  = 10000000;
	
	public static void main(String[] args) throws Exception{
		
		
		File file= new File("synthetic.csv");
		
		BufferedWriter bf = new BufferedWriter(new FileWriter(file));
		int k1;
		String k2;
		for(int i=0 ; i<LIMIT ; i++){
			k1=(int)(Math.random()*10000000);
			k2=RandomStringUtils.randomAlphanumeric(10);
			bf.write(k1+"\t"+k2+"\n");
		}
		
		bf.close();
	}

}
