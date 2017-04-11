/**
 * Created by connecterra on 2/9/17.
 */
// Include the following imports to use blob APIs.
import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;

import java.io.*;

public class BlobDownloader {
    public static final String storageConnectionString =
            "DefaultEndpointsProtocol=http;" +
                    "AccountName=ctraeventseu;" +
                    "AccountKey=BDSW8f7w3Ujois7AAuWAD5czcM+9RM1k68C9RPmL1RRBV3z8L+ovod6LCRPGFB6FP0129fi6+iA0K/zQD/juAQ==";
    public static void main(String[] args) {
        long startProgram = System.currentTimeMillis() /1000;
        try {
            long startEpoch = 0;
            long endEpoch = System.currentTimeMillis()/1000;

            String destination = "~/";
            String usage = "BlobDownloader.jar [--start start-epoch] [--end end-epoch] --destination destination-folder-path";
            if(args != null){
                if(args.length %2 == 0 ) {
                    for (int i = 0; i < args.length; i++) {
                        if(args[i].equals("--start")) {
                            startEpoch = Long.parseLong((args[i + 1]));
                        }
                        if(args[i].equals("--end")){
                            endEpoch = Long.parseLong(args[i+1]);
                        }
                        if(args[i].equals("--destination")){
                            destination = args[i+1];
                            if(destination.charAt(destination.length()-1)!= '/'){
                                destination = destination + "/";
                            }
                        }
                    }
                } else {
                    System.out.println(usage);
                    System.exit(1);
                }
            } else {
                System.out.println(usage);
                System.exit(1);
            }
            System.out.println("input arguments processed correctly");
            System.out.println(">>> start:\t" + startEpoch);
            System.out.println(">>> end:\t" +endEpoch);
            System.out.println(">>> dst:\t" + destination);

            System.out.println("normalizing epochs to the nearest 24 hr interval UTC");
            long startNormalized = startEpoch - (startEpoch%86400);
            long endNormalized = endEpoch - (endEpoch%86400);
            // Retrieve storage account from connection-string.
            System.out.println("connecting to azure storage ...");
            CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);

            // Create the blob client.
            CloudBlobClient blobClient = storageAccount.createCloudBlobClient();

            // Get a reference to a container.
            // The container name must be lower case
            CloudBlobContainer container = blobClient.getContainerReference("csv-container");

            System.out.println("iterating through blobs in the cloud");
            // Loop through each blob item in the container.
            for (ListBlobItem blobItem : container.listBlobs()) {
                // If the item is a blob, not a virtual directory.
                //System.out.println(blobItem.getUri());
                if (blobItem instanceof CloudBlobDirectory ) {
                    String prefix = ((CloudBlobDirectory) blobItem).getPrefix();
                    long currentEpoch = Long.parseLong(prefix.replace("/",""));
                    if((currentEpoch >= startNormalized) && (currentEpoch <=endNormalized)){
                        // these are the ones to copy
                        for (ListBlobItem csvfileBlob : container.listBlobs(prefix,true)){
                            System.out.println("blob matched time range: " + csvfileBlob.getUri());
                            CloudAppendBlob appendBlob = (CloudAppendBlob)csvfileBlob;
                            try {
                                String filePath = destination   + appendBlob.getName();

                                int folderEnd = filePath.lastIndexOf('/');
                                String subDirectory = filePath.substring(0,folderEnd);
                                System.out.println(subDirectory);
                                File subdirs = new File(subDirectory);
                                System.out.println("creating subdirectory: " + subdirs);
                                subdirs.mkdirs();
                                System.out.println("creating file locally: "+filePath);
                                File file = new File(filePath);
                                if (file.createNewFile()){
                                    System.out.println(">>> file is created!");
                                }else{
                                    System.out.println(">>> file already exists.");
                                }
                                System.out.println("downloading blob ...");
                                long startDownload = System.currentTimeMillis()/1000;
                                //appendBlob.download(new FileOutputStream(filePath));
                                appendBlob.download(new BufferedOutputStream(new FileOutputStream(filePath)));
                                long endDownload = System.currentTimeMillis()/1000;
                                System.out.println("download complete. time taken: " + (endDownload-startDownload) + " s\n");
                            }catch (FileNotFoundException fex){
                                fex.printStackTrace();
                            }
                        }
                    }
                }
            }

        }
        catch (Exception e)
        {
            // Output the stack trace.
            e.printStackTrace();
        }

        long endProgram = System.currentTimeMillis()/1000;
        System.out.println("total runtime: " + (endProgram-startProgram) +  " s");

    }
}
