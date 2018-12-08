package blockingQueue;

import java.io.*;
import java.nio.charset.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

/**
 * @version 1.03 2018-03-17
 * @author Cay Horstmann
 */
public class BlockingQueueTest
{
   //Maximum capacity of the BlockingQueue
   //10 file objects is the most that can
   //be in the Queue at one time.
   private static final int FILE_QUEUE_SIZE = 10;

   //Number of threads that will pull off a File
   //from the filesQueue and then do a search of
   //that file for the keyword.
   private static final int SEARCH_THREADS = 100;

   //Marker to indicate that there are no more
   //Files in the filesQueue
   private static final Path DUMMY = Paths.get("");


   private static BlockingQueue<Path> filesQueue = new ArrayBlockingQueue<>(FILE_QUEUE_SIZE);

   public static void main(String[] args)
   {
      try (var in = new Scanner(System.in)) 
      {
         System.out.print("Enter base directory (e.g. /opt/jdk-9-src): ");

         String directory = in.nextLine();

         System.out.print("Enter keyword (e.g. volatile): ");

         String keyword = in.nextLine();

         /*
         This thread will find all the files in the directory
         provided above and its sub-directories and place
         each file in the filesQueue
          */
         Runnable loadFilesQueue = () -> {
            try
            {

               enumerate(Paths.get(directory));

               //When finished put the marker that indicates the filesQueue is now empty.
               filesQueue.put(DUMMY);
            }
            catch (IOException e)
            {
               e.printStackTrace();
            }
            catch (InterruptedException e)
            {
            }            
         };
         
         new Thread(loadFilesQueue).start();


         /*
         Create a separate thread for each value
         in SEARCH_THREADS.
          */
         for (int i = 1; i <= SEARCH_THREADS; i++) {

            /*
            This thread will take a File from the
            filesQueue and perform a search on that
            file for the keyword provided above.
             */
            Runnable searcher = () -> {
               try
               {
                  var done = false;

                  while (!done)
                  {
                     Path file = filesQueue.take();

                     /*
                     If the marker that indicates the
                     filesQueue is empty is reached
                     put the marker back on the filesQueue
                     so other threads will see it.
                     Then change done to true so this
                     loop will end.
                      */
                     if (file == DUMMY)
                     {
                        filesQueue.put(file);
                        done = true;
                     }
                     else {

                        search(file, keyword);

                     }
                  }
               }
               catch (IOException e)
               {
                  e.printStackTrace();
               }
               catch (InterruptedException e)
               {
               }
               
            };

            new Thread(searcher).start();
         }
      }
   }
   
   /**
    * Recursively enumerates all files in a given directory and its subdirectories.
    * See Chapters 1 and 2 of Volume II for the stream and file operations.
    * @param directory the directory in which to start
    */
   public static void enumerate(Path directory) throws IOException, InterruptedException
   {
      try (Stream<Path> children = Files.list(directory))
      {
         for (Path child : children.collect(Collectors.toList())) 
         {
            if (Files.isDirectory(child))
               enumerate(child);
            else
               filesQueue.put(child);
         }
      }
   }
   
   /**
    * Searches a file for a given keyword and prints all matching lines.
    * @param file the file to search
    * @param keyword the keyword to search for
    */
   public static void search(Path file, String keyword) throws IOException
   {
      try (var in = new Scanner(file, StandardCharsets.UTF_8))
      {
         int lineNumber = 0;
         while (in.hasNextLine())
         {
            lineNumber++;
            String line = in.nextLine();
            if (line.contains(keyword)) {
               System.out.print(Thread.currentThread() + " - ");
               System.out.printf("%s:%d:%s%n \n", file, lineNumber, line);
            }
         }
      }
   }
}
