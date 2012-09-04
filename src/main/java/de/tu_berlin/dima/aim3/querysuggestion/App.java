package de.tu_berlin.dima.aim3.querysuggestion;

import java.util.ArrayList;
import java.util.List;



/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
//    	
//		List<List<Integer>> clusters = new ArrayList<List<Integer>>();
//		List<Integer> singleCluster = new ArrayList<Integer>();
//		singleCluster.add(1);
//		clusters.add(singleCluster);
//		System.out.println(clusters);
    	
		int rowCount = 5;
		int columnCount = 10;
//		Matrix probMatrix = new Matrix(rowCount ,columnCount );
		Matrix probMatrix = new Matrix(rowCount ,columnCount,3 );
		printMatrix(probMatrix.getMatrix(1, 1, 1, 3));
//		double f = 1 / rowCount;
//		f = 0.5;
//		/** set values in big matrix */
//		// first ref co occ
//		
////		probMatrix.setMatrix(0, 0, rowCount - 1, rowCount -1, normCoOccMatrix);
//		// than doc ref 
////		probMatrix.setMatrix(0, rowCount -1 ,rowCount -1 ,normDocMatrix.getColumnDimension(), normDocMatrix);
//		// make matrix for f column
//		Matrix fMatrix = new Matrix(rowCount,1,f);
//		probMatrix.set(0, columnCount-1, 50);
//		printMatrix(probMatrix);
//		System.out.println();
//		printMatrix(fMatrix);
//		System.out.println(0 +" " +(columnCount - 1) +" " + (rowCount - 1) +" " +(columnCount - 1));
////		probMatrix.setMatrix(0,columnCount - 1,rowCount - 1,columnCount - 1,fMatrix);
//		printMatrix(probMatrix.getMatrix(0,0,rowCount - 1,columnCount - 1));
//		probMatrix.setMatrix(0,rowCount - 1,columnCount - 1,columnCount - 1,fMatrix);
//		printMatrix(probMatrix);
//		
    }
    
	private static void printMatrix(Matrix A) {
		for (int i = 0; i < A.getRowDimension(); i++) {
			for (int j = 0; j < A.getColumnDimension(); j++) {
				System.out.print(A.get(i, j) + "\t");
			}
			System.out.println();
		}
	}
}
