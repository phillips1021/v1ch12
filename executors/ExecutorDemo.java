package executors;

import java.io.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

/**
 * This program demonstrates the Callable interface and executors.
 * @version 1.0 2018-01-04
 * @author Cay Horstmann
 */
public class ExecutorDemo
{
   /**
    * Counts occurrences of a given word in a file.
    * @return the number of times the word occurs in the given word.
    */
   public static long occurrences(String word, Path path)
   {
      try (var in = new Scanner(path))
      {
         int count = 0;
         while (in.hasNext())
            if (in.next().equals(word)) count++;
         return count;
      }
      catch (IOException ex)
      {
         return 0;
      }
   }

   /**
    * Returns all descendants of a given directory--see Chapters 1 and 2 of
    * Volume II
    * @param rootDir the root directory
    * @return a set of all descendants of the root directory
    */
   public static Set<Path> descendants(Path rootDir) throws IOException
   {
      try (Stream<Path> entries = Files.walk(rootDir))
      {
         return entries.filter(Files::isRegularFile)
               .collect(Collectors.toSet());
      }
   }

   /**
    * Yields a task that searches for a word in a file.
    * @param word the word to search
    * @param path the file in which to search
    * @return the search task that yields the path upon success
    */
   public static Callable<Path> searchForTask(String word, Path path)
   {
      return () -> {
         try (var in = new Scanner(path))
         {
            while (in.hasNext())
            {
               if (in.next().equals(word)) return path;
               if (Thread.currentThread().isInterrupted())
               {
                  System.out.println("Search in " + path + " canceled.");
                  return null;
               }
            }
            throw new NoSuchElementException();
         }
      };
   }

   public static void main(String[] args)
      throws InterruptedException, ExecutionException, IOException
   {
      try (var in = new Scanner(System.in))
      {
         /*
         PART 1: Get the total number of times a word is found in a
         collection of files.
          */
         System.out.print("Enter base directory (e.g. /opt/jdk-9-src): ");

         String start = in.nextLine();

         System.out.print("Enter keyword (e.g. volatile): ");

         String word = in.nextLine();
      
         Set<Path> files = descendants(Paths.get(start));

         System.out.printf("Will search %d files for the word %s \n", files.size(), word);

         /*
         Create a collection of Callable objects - this is
         used to store all the tasks that must be run.
          */
         var tasks = new ArrayList<Callable<Long>>();

         for (Path file : files)
         {
            /*
            For each File create a Callable object that will return
            a Long object that is the total number of times
            the provided word is found in the provided file.
             */
            Callable<Long> task = () -> occurrences(word, file);


            tasks.add(task);

         }

         /*
         Create a thread pool that will execute each task immediately
         using an existing idle thread when available or creating
         a new thread if no threads are idle.
          */
         ExecutorService executor = Executors.newCachedThreadPool();

         // Use a single thread executor instead to see if multiple threads
         // speed up the search
         //ExecutorService executor = Executors.newSingleThreadExecutor();
         
         Instant startTime = Instant.now();

         /*
         Submit all the Callable objects to ExecutorService.
         This is blocking until all tasks have completed.
         A collection of Future objects is returned.  The Future
         object stores the total number of times the word
         was found in the file.
          */
         List<Future<Long>> results = executor.invokeAll(tasks);

         long total = 0;

         /*
         Iterate over all the Future objects and get the
         total of how many times the word was found in ALL
         files.
          */
         for (Future<Long> result : results) {

            total += result.get();

         }

         Instant endTime = Instant.now();

         System.out.println("Occurrences of " + word + ": " + total);

         System.out.println("Time elapsed: " + Duration.between(startTime, endTime).toMillis() + " ms");

         /*
         PART 2: Find the first file that contains the provided word.
          */

         var searchTasks = new ArrayList<Callable<Path>>();

         for (Path file : files) {

            searchTasks.add(searchForTask(word, file));

         }

         /*
         Pass the collection of Callable objects to the thread pool
         ExecutorService.  This call will block until one task
         returns.  Once one task returns all other running threads
         are interrupted and canceled.
          */
         Path found = executor.invokeAny(searchTasks);

         System.out.println(word + " occurs in: " + found);


         /*
         PART 3: Display how many threads were created by
         the ExecutorService.
          */
         if (executor instanceof ThreadPoolExecutor) {

            // the single thread executor isn't
            System.out.println("Largest pool size: "
                    + ((ThreadPoolExecutor) executor).getLargestPoolSize());

         }

         executor.shutdown();

      }
   }
}