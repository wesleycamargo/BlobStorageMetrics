using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Configuration;


namespace consoleUploadFullFramework
{
    class Program
    {
        static async Task Main()
        {
            string containerName = ConfigurationManager.AppSettings["containerName"];
            string uploadPath = ConfigurationManager.AppSettings["uploadPath"];

            var date = DateTime.Now.ToString("yyyyMMdd-hhmmss");

            string logDir = String.Format(ConfigurationManager.AppSettings["logDir"], date);
            string stgCnnStr = ConfigurationManager.AppSettings["stgCnnStr"];

            bool deleteContainer = Convert.ToBoolean(ConfigurationManager.AppSettings["deleteContainer"]);
            bool consoleLog = Convert.ToBoolean(ConfigurationManager.AppSettings["consoleLog"]);

            int qtdThreads = Convert.ToInt32(ConfigurationManager.AppSettings["qtdThreads"]);

            var account = CloudStorageAccount.Parse(stgCnnStr);
            var cloudBlobClient = account.CreateCloudBlobClient();

            string fullContainerName = String.Format("{0}-{1}", containerName, date);

            CloudBlobContainer container = cloudBlobClient.GetContainerReference(fullContainerName);

            container.CreateIfNotExists();

            uploadPath = CopyFiles(uploadPath);

            try
            {
                Console.WriteLine("Iterating in directory: {0}", uploadPath);
                int count = 0;
                int max_outstanding = 100;
                int completed_count = 0;

                BlobRequestOptions options = new BlobRequestOptions
                {
                    ParallelOperationThreadCount = qtdThreads,
                    DisableContentMD5Validation = true,
                    StoreBlobContentMD5 = false
                };

                SemaphoreSlim sem = new SemaphoreSlim(max_outstanding, max_outstanding);

                List<Task> tasks = new List<Task>();

                var totalFiles = Directory.GetFiles(uploadPath).Count();

                Console.WriteLine("Found {0} file(s)", totalFiles);
                File.AppendAllText(logDir, String.Format("Found {0} file(s){1}", totalFiles, Environment.NewLine));

                Console.WriteLine("Container Name: {0}", fullContainerName);

                File.AppendAllText(logDir, String.Format("Container Name: {0} {1}", fullContainerName, Environment.NewLine));

                Stopwatch time = Stopwatch.StartNew();

                Decimal rating = 0;

                foreach (string path in Directory.GetFiles(uploadPath))
                {
                    // Create random file names and set the block size that is used for the upload.
                    //var container = containers[count % 5];
                    string fileName = Path.GetFileName(path);
                    //Console.WriteLine("Uploading {0} to container {1}.", path, container.Name);
                    File.AppendAllText(logDir, String.Format("Uploading {0} to container {1}.{2}", path, container.Name, Environment.NewLine));
                    CloudBlockBlob blockBlob = container.GetBlockBlobReference(fileName);

                    // Set block size to 100MB.
                    blockBlob.StreamWriteSizeInBytes = 100 * 1024 * 1024;
                    await sem.WaitAsync();

                    // Create tasks for each file that is uploaded. This is added to a collection that executes them all asyncronously.  
                    tasks.Add(blockBlob.UploadFromFileAsync(path, null, options, null).ContinueWith((t) =>
                    {
                        sem.Release();
                        Interlocked.Increment(ref completed_count);
                    }));
                    count++;



                    int secondsElapsed = (int)time.Elapsed.TotalSeconds;



                    if (secondsElapsed > 0)
                    {
                        rating = Decimal.Divide(count, secondsElapsed);
                    }

                    //Console.WriteLine("Time elapsed:{0} - Files per second: {1} - Files uploaded: {2}", time.Elapsed, rating, count);
                    File.AppendAllText(logDir, String.Format("Time elapsed:{0} - Files per second: {1} - Files uploaded: {2}{3}", time.Elapsed.TotalSeconds, rating, count, Environment.NewLine));

                    if (consoleLog)
                    {
                        Console.WriteLine("Uploaded files: {0} from {1}", count, totalFiles);
                    }

                }

                await Task.WhenAll(tasks);

                time.Stop();

                int containerItems = container.ListBlobs().Count();

                Console.WriteLine("Upload has been completed in {0} seconds. - Container uploaded items: {1}", time.Elapsed.TotalSeconds.ToString(), containerItems);
                File.AppendAllText(logDir, String.Format("Upload has been completed in {0} seconds. Files per second:{1}{2} - Container uploaded items: {3}", time.Elapsed.TotalSeconds.ToString(), rating, Environment.NewLine, containerItems));

                

            }
            catch (DirectoryNotFoundException ex)
            {
                File.AppendAllText(logDir, String.Format("Error parsing files in the directory: {0}{1}", ex.Message, Environment.NewLine));

                Console.WriteLine("Error parsing files in the directory: {0}", ex.Message);
            }
            catch (Exception ex)
            {
                File.AppendAllText(logDir, String.Format("Error parsing files in the directory: {0}{1}", ex.Message, Environment.NewLine));

                Console.WriteLine(ex.Message);
            }
            finally
            {
                Console.WriteLine("Excluding Blob container and copy files...");
                if (deleteContainer)
                {
                    container.Delete();
                }

                RemoveCopyFiles(uploadPath);

            }

        }

        private static void RemoveCopyFiles(string uploadPath)
        {

            if (Directory.Exists(uploadPath))
                Directory.Delete(uploadPath, true);
        }

        private static string CopyFiles(string uploadPath)
        {

            var qtdFiles = Convert.ToInt32(ConfigurationManager.AppSettings["qtdFiles"]);

            var file = Directory.GetFiles(uploadPath).First();

            

            var copyPath = String.Format(@"{0}\copy\", Path.GetDirectoryName(file));

            var count = 1;
            while (count < qtdFiles+1)
            {
                var fileName = String.Format("{0}-{1}", count, Path.GetFileName(file));

                if (!System.IO.Directory.Exists(copyPath))
                    System.IO.Directory.CreateDirectory(copyPath);

                var fullName = String.Format(@"{0}\{1}", copyPath, fileName);

                File.Copy(file, fullName);

                count++;
            }

            return copyPath;
        }
    }
}
