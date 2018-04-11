package Demo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class QuickSortTest {
	

	    public static void main(String[] args) {
	        int[] A = {9, 3, 10, 4, 1, 44, 12, 2, 90, 0};
	        byte [] B = {0,0,0,125,67,98,97,99,114};
	        int l = 0;
	        int r = A.length-1;
	        int rr = B.length-1;
	        QuickSort(A, l, r);
	        QuickSortByte(B,l,rr);
	        for (int i = 0; i < A.length; i++){
	            System.out.println(A[i] + " ");
	        }
	        for (int i = 0; i < B.length; i++){
	            System.out.print(B[i] + " ");
	        }
	    }

	    private static void QuickSort(int[] a, int l, int r) {
	        int i;
	        if (r > l){
	            i = partition(a, l, r);
	            QuickSort(a, l, i-1);
	            QuickSort(a, i+1, r);
	        }
	    }
	    
	    private static void QuickSortByte(byte[] b, int l, int r) {
	        int i;
	        if (r > l){
	            i = partitionByte(b, l, r);
	            QuickSortByte(b, l, i-1);
	            QuickSortByte(b, i+1, r);
	        }
	    }

	    private static int partition(int[] a, int l, int r) {
	        int v = a[r];
	        int i = l;
	        int j = r;
	        int temp;
	        while (i < j){
	            while (a[i] < v){
	                i = i + 1;
	            }
	            while ((i < j) && (a[j] >= v)){
	                j = j - 1;
	            }
	            temp = a[i];
	            if (i < j){
	                a[i] = a[j];
	                a[j] = temp;
	            }else{
	                a[i] = a[r];
	                a[r] = temp;
	            }
	        }
	        return i;
	    }
	    
	    private static byte partitionByte(byte[] b, int l, int r) {
	        int v = b[r];
	        int i = l;
	        int j = r;
	        int temp;
	        while (i < j){
	            while (b[i] < v){
	                i = i + 1;
	            }
	            while ((i < j) && (b[j] >= v)){
	                j = j - 1;
	            }
	            temp = b[i];
	            if (i < j){
	                b[i] = b[j];
	                b[j] = (byte) temp;
	            }else{
	                b[i] = b[r];
	                b[r] = (byte) temp;
	            }
	        }
	        return (byte) i;
	    }
	}