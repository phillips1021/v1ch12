package forkJoin;

import java.util.concurrent.*;
import java.util.function.*;

/**
 * This program demonstrates the fork-join framework.
 * @version 1.01 2015-06-21
 * @author Cay Horstmann
 */
public class ForkJoinTest
{
   /**
    * Determine the total number of elements in an array
    * where the element value is > 0.5.
    * @param args
    */
   public static void main(String[] args)
   {
      //number of elements in the array
      final int SIZE = 10_000_000;

      var numbers = new double[SIZE];

      /*
      Place a random value greater the 0.0 but less then 1.0
      in each array element.
       */
      for (int i = 0; i < SIZE; i++) {

         numbers[i] = Math.random();

      }

      /*
      Create an object of the RecursiveTask that the ForkJoinPool can break
      down and execute using multiple threads.
       */
      var counter = new Counter(numbers, 0, numbers.length, x -> x > 0.5);

      var pool = new ForkJoinPool();

      pool.invoke(counter);

      System.out.println(counter.join());
   }
}

class Counter extends RecursiveTask<Integer>
{
   public static final int THRESHOLD = 1000;
   private double[] values;
   private int from;
   private int to;
   private DoublePredicate filter;

   public Counter(double[] values, int from, int to, DoublePredicate filter)
   {
      this.values = values;
      this.from = from;
      this.to = to;
      this.filter = filter;
   }

   protected Integer compute()
   {
      /*
      If the remaining array values to process are less then 1000
      then stop doing the recursion and calculate the result.
       */
      if (to - from < THRESHOLD)
      {
         int count = 0;
         for (int i = from; i < to; i++)
         {
            if (filter.test(values[i])) count++;
         }
         return count;
      }
      else
      {
         /*
         Divide up the array elements that need to be processed
         and process those on separate threads.
          */
         int mid = (from + to) / 2;
         var first = new Counter(values, from, mid, filter);
         var second = new Counter(values, mid, to, filter);
         invokeAll(first, second);
         return first.join() + second.join();
      }
   }
}