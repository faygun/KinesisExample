using Amazon;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KinesisConsole
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Main done...");
            PushData("ahmet", "1989");
            ReadFromStream();
            Console.ReadKey();
        }

        private static string PushData(string name, string userId)
        {
            try
            {
                var kinesis = new AmazonKinesisClient("XXXXX", "YYYYYY", RegionEndpoint.EUWest1);

                var mo = new { Name = name, UserId = userId };
                var bytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(mo));
                var ms = new MemoryStream(bytes);

                Random rnd = new Random(100);
                var request = new PutRecordRequest
                {
                    StreamName = "your-kinesis-name",
                    PartitionKey = "first-my-partion",
                    Data = ms

                };

                var kinesisResponse = kinesis.PutRecord(request);
            }
            catch (Exception ex)
            {
                return ex.Message;
            }

            return "OK";
        }
        private static void ReadFromStream()
        {
            /*Config example*/
            //AmazonKinesisConfig config = new AmazonKinesisConfig();
            //config.RegionEndpoint = Amazon.RegionEndpoint.EUWest1;

            //AmazonKinesisClient kinesisClient = new AmazonKinesisClient(config);

            /*instance example*/
            var kinesisClient = new AmazonKinesisClient("XXXXX", "YYYYYY", RegionEndpoint.EUWest1);
            String kinesisStreamName = "your-kinesis-name";

            DescribeStreamRequest describeRequest = new DescribeStreamRequest();
            describeRequest.StreamName = kinesisStreamName;

            DescribeStreamResponse describeResponse = kinesisClient.DescribeStream(describeRequest);
            List<Shard> shards = describeResponse.StreamDescription.Shards;

            foreach (Shard shard in shards)
            {
                GetShardIteratorRequest iteratorRequest = new GetShardIteratorRequest();
                iteratorRequest.StreamName = kinesisStreamName;
                iteratorRequest.ShardId = shard.ShardId;
                iteratorRequest.ShardIteratorType = ShardIteratorType.TRIM_HORIZON;

                GetShardIteratorResponse iteratorResponse = kinesisClient.GetShardIterator(iteratorRequest);
                string iteratorId = iteratorResponse.ShardIterator;

                while (!string.IsNullOrEmpty(iteratorId))
                {
                    GetRecordsRequest getRequest = new GetRecordsRequest();
                    getRequest.Limit = 1000;
                    getRequest.ShardIterator = iteratorId;

                    GetRecordsResponse getResponse = kinesisClient.GetRecords(getRequest);
                    string nextIterator = getResponse.NextShardIterator;
                    List<Record> records = getResponse.Records;

                    if (records.Count > 0)
                    {
                        Console.WriteLine("Received {0} records. ", records.Count);
                        foreach (Record record in records)
                        {
                            string json = Encoding.UTF8.GetString(record.Data.ToArray());
                            Console.WriteLine("Json string: " + json);
                        }
                    }
                    iteratorId = nextIterator;
                }
            }
        }
    }
}
