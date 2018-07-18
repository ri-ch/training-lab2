using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Amazon;
using Amazon.Extensions.NETCore.Setup;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.Extensions.Configuration;

namespace S3Lab
{
    class Program
    {
        private static IConfiguration _config;
        private static Random _random = new Random();

        static void Main(string[] args)
        {
            _config = 
                new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json")
                .Build();

            var options = _config.GetAWSOptions();
            IAmazonS3 client = options.CreateServiceClient<IAmazonS3>();

            Console.WriteLine("Running....");

            IEnumerable<String> strings = TransformData(client).Result;

            Console.WriteLine("Done");
            Console.ReadLine();
        }

        private static async Task<IEnumerable<string>> TransformData(IAmazonS3 client)
        {
            string outputBucket = _config["OutputBucket"];

            await Setup(client);

            ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request();
            listObjectsRequest.BucketName = _config["InputBucket"];

            ListObjectsV2Response listResponse = client.ListObjectsV2Async(listObjectsRequest).Result;

            if (listResponse.KeyCount == 0)
            {
                Console.WriteLine("No items found in input bucket");
                return null;
            }
            else
            {
                Console.WriteLine($"Transforming {listResponse.KeyCount} objects");
            }

            List<string> urls = new List<string>();

            foreach (S3Object inputItem in listResponse.S3Objects)
            {
                GetObjectResponse response = 
                    await client.GetObjectAsync(
                        inputItem.BucketName,
                        inputItem.Key
                    );

                PutObjectRequest putRequest = new PutObjectRequest();
                putRequest.BucketName = outputBucket;

                string outputKey = $"{inputItem.Key.Split("-")[0]}-output";

                using (TextReader reader = new StreamReader(response.ResponseStream))
                {
                    putRequest.ContentBody = Convert.ToBase64String(Encoding.UTF8.GetBytes(await reader.ReadToEndAsync()));
                    putRequest.ContentType = "text/plain";
                    putRequest.Key = outputKey;
                }

                await client.PutObjectAsync(putRequest);

                GetPreSignedUrlRequest presignedRequest = new GetPreSignedUrlRequest();
                presignedRequest.BucketName = outputBucket;
                presignedRequest.Key = outputKey;
                presignedRequest.Expires = DateTime.Now.AddMinutes(30);

                urls.Add(client.GetPreSignedURL(presignedRequest);
            }

            return urls;
        }

        private static async Task Setup(IAmazonS3 client)
        {
            ListObjectsV2Request listRequest = new ListObjectsV2Request();
            listRequest.BucketName = _config["InputBucket"];
            var result = await client.ListObjectsV2Async(listRequest);

            if (result.KeyCount != 0)
                return;

            // Adds test data to the input bucket
            string[] data = new[] {
                "This is a string",
                "This is another string",
                "This is a third string"
            };

            foreach (string s in data)
            {
                PutObjectRequest request = new PutObjectRequest();
                request.ContentBody = s;
                request.BucketName = _config["InputBucket"];
                request.ContentType = "text/plain";
                request.Key = $"{_random.Next(100, 999)}-file";
                await client.PutObjectAsync(request);
            }
        }
    }
}
