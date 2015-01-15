//package SQLCertify;

import java.util.*;

public class ItemCombination {

  private static List<String> comb;
private static List<List<String>> comb_res;

//public static void main(String[] args) {	
	//String hello[] = {"K1", "K2", "K3"};
	//List<List<String>> res = StringCombination(hello);
	//System.out.println(res.toString());
  //}

  
  public List<List<String>> StringCombination(String[] iterms)
  {
	  comb_res = new ArrayList<List<String>>();
	  
	  int iterm_length = iterms.length;
	  for(int i=0; i<iterm_length; ++i){
		  Combination c = new Combination(iterm_length,i+1);
		    while (c.hasNext()) {
		      int[] a = c.next();
		      //System.out.println(Arrays.toString(a));
		      comb = new ArrayList<String>();
		      for(int j=0; j<a.length; ++j)
		    	  comb.add(iterms[a[j]].toString());
		      comb_res.add(comb);
		      
		    }
	  }
	return comb_res;
  }
  
 /* public static void test1() {
    Combination c = new Combination(5,5);
    while (c.hasNext()) {
      int[] a = c.next();
      System.out.println(Arrays.toString(a));
    }
  }
  
  public static void test2() {
    Scanner scanner = new Scanner(System.in);
    System.out.print("Enter n: ");
    int n = scanner.nextInt();
    System.out.print("Enter r: ");
    int r = scanner.nextInt();
    Combination c = new Combination(n,r);
    System.out.println("Here are all the ways you can combine " +
                        r + " choices among " + n + " objects:");
    int counter = 0;
    while (c.hasNext()) {
      int[] next = c.next();
      System.out.println(Arrays.toString(next));
      counter++;
    }
  */ 
  //System.out.println("total = " + n + "C" + r + " = " + counter);
  //}
}
