
using ConfigHelper;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection.Metadata;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AdminkafkaCli
{
    public static class KafkaOpera
    {

        public static string bootstrapServers = ConfigVals.BrootStrapServer;

        static string ToString(int[] array) => $"[{string.Join(", ", array)}]";

        public static IAdminClient CreateAdminClient()
        {
            return new AdminClientBuilder(new Dictionary<string, string>()
            {
                {"bootstrap.servers", bootstrapServers}
            }).Build();
        }

        public static async Task<List<ListConsumerGroupOffsetsResult>> AlterConsumerGroupOffsetsAsync(string group)
        {

            var topics = GetTopicMetadata();
            var tpes = new List<TopicPartition>();
            foreach (var topicmeta in topics.Topics)
            {
                foreach (var parti in topicmeta.Partitions)
                {
                    tpes.Add(new TopicPartition(topicmeta.Topic, parti.PartitionId));
                }
            }         

            var input = new List<ConsumerGroupTopicPartitions>() { new ConsumerGroupTopicPartitions(group, tpes) };
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                try
                {
                    var result = await adminClient.ListConsumerGroupOffsetsAsync(input);
                    Console.WriteLine("Successfully listed offsets:");
                    foreach (var groupResult in result)
                    {

                        Console.WriteLine(groupResult);
                    }
                    return result;

                }
                catch (AlterConsumerGroupOffsetsException e)
                {
                    Console.WriteLine($"An error occurred altering offsets: {(e.Results.Any() ? e.Results[0] : null)}");
                    Environment.ExitCode = 1;
                    return null;
                }
                catch (ListConsumerGroupOffsetsException e)
                {
                    Console.WriteLine($"An error occurred listing offsets: {(e.Results.Any() ? e.Results[0] : null)}");
                    Environment.ExitCode = 1;
                    return null;

                }
                catch (KafkaException e)
                {
                    Console.WriteLine("An error occurred listing consumer group offsets." +
                        $" Code: {e.Error.Code}" +
                        $", Reason: {e.Error.Reason}");
                    Environment.ExitCode = 1;
                    return null;

                }
            }

        }

        public static async Task<ListConsumerGroupsResult> ListGroups()
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                var timeout = TimeSpan.FromSeconds(30);
                var statesList = new List<ConsumerGroupState>();
                try
                {
                    var result = await adminClient.ListConsumerGroupsAsync(new ListConsumerGroupsOptions()
                    {
                        RequestTimeout = timeout,
                        MatchStates = statesList,
                    });
                    Console.WriteLine(result);
                    return result;
                }
                catch (KafkaException e)
                {
                    Console.WriteLine("An error occurred listing consumer groups." +
                        $" Code: {e.Error.Code}" +
                        $", Reason: {e.Error.Reason}");
                    Environment.ExitCode = 1;
                    return null;
                }

            }
        }

        public static Metadata GetTopicMetadata()
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                // Warning: The API for this functionality is subject to change.
                var meta = adminClient.GetMetadata(TimeSpan.FromSeconds(20));
                //var groups = adminClient.ListGroups(TimeSpan.FromSeconds(10));
                Console.WriteLine($"{meta.OriginatingBrokerId} {meta.OriginatingBrokerName}");
                meta.Brokers.ForEach(broker =>
                    Console.WriteLine($"Broker: {broker.BrokerId} {broker.Host}:{broker.Port}"));

                meta.Topics.ForEach(topic =>
                {
                    Console.WriteLine($"Topic: {topic.Topic} {topic.Error}");
                    topic.Partitions.ForEach(partition =>
                    {
                        Console.WriteLine($"  Partition: {partition.PartitionId}");
                        Console.WriteLine($"    Replicas: {ToString(partition.Replicas)}");
                        Console.WriteLine($"    InSyncReplicas: {ToString(partition.InSyncReplicas)}");
                    });
                });

                return meta;
            }
        }




        public static async Task CreateTopicAsync(string topic, int num)
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                try
                {
                    await adminClient.CreateTopicsAsync(new TopicSpecification[] {
                        new TopicSpecification { Name = topic, ReplicationFactor = 3, NumPartitions = num } });
                }
                catch (CreateTopicsException e)
                {
                    Console.WriteLine($"An error occurred creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
                }
            }
          
        }

  

        public static async Task DeleteAclsAsync(string[] topics)
        {

            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                try
                {

                    await adminClient.DeleteTopicsAsync(topics);
                   
                }
                catch (DeleteTopicsException e)
                {
                    Console.WriteLine("One or more create ACL operations failed.");
                    for (int i = 0; i < e.Results.Count; ++i)
                    {
                        var result = e.Results[i];
                        if (!result.Error.IsError)
                        {
                            Console.WriteLine($"Deleted ACLs in operation {i}");
                        }
                        else
                        {
                            Console.WriteLine($"An error occurred in delete ACL operation {i}: Code: {result.Error.Code}" +
                                $", Reason: {result.Error.Reason}");
                        }
                    }
                }
                catch (KafkaException e)
                {
                    Console.WriteLine($"An error occurred calling the DeleteAcls operation: {e.Message}");
                }
            }
        }

      
    }

}
