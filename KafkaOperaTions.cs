
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
            //var tpoes = new List<TopicPartitionOffset>();
            //tpoes.Add(new TopicPartitionOffset("20230404_TestRaid", 0, 0));

            //for (int i = 1; i < topics.Topics.Count; i += 1)
            //{
            //    try
            //    {
            //        var topic = topics.Topics[i].Topic;
            //        var partition = 0;
            //        var offset = 0;
            //        tpoes.Add(new TopicPartitionOffset(topic, partition, offset));
            //    }
            //    catch (Exception e)
            //    {
            //        Console.Error.WriteLine($"An error occurred while parsing arguments: {e}");
            //        Environment.ExitCode = 1;
            //        return;
            //    }
            //}

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
                        //var tpoes = new List<TopicPartitionOffset>();
                        //tpoes.Add(new TopicPartitionOffset(groupResult.Partitions[0].TopicPartition, 10000));
                        //var p2 = new List<ConsumerGroupTopicPartitionOffsets>() { new ConsumerGroupTopicPartitionOffsets(group, tpoes) };
                        //var results = await adminClient.AlterConsumerGroupOffsetsAsync(p2);
                        //Console.WriteLine("Successfully altered offsets:");
                        //foreach (var item in results)
                        //{
                        //    Console.WriteLine(item);
                        //}
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
            //using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            //{
            //    try
            //    {
            //        var results = await adminClient.AlterConsumerGroupOffsetsAsync(input);
            //        Console.WriteLine("Successfully altered offsets:");
            //        foreach (var groupResult in results)
            //        {
            //            Console.WriteLine(groupResult);
            //        }

            //    }
            //    catch (AlterConsumerGroupOffsetsException e)
            //    {
            //        Console.WriteLine($"An error occurred altering offsets: {(e.Results.Any() ? e.Results[0] : null)}");
            //        Environment.ExitCode = 1;
            //        return;
            //    }
            //    catch (KafkaException e)
            //    {
            //        Console.WriteLine("An error occurred altering consumer group offsets." +
            //            $" Code: {e.Error.Code}" +
            //            $", Reason: {e.Error.Reason}");
            //        Environment.ExitCode = 1;
            //        return;
            //    }
            //}
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


        //static List<AclBinding> ParseAclBindings(string[] args, bool many)
        //{
        //    var numCommandArgs = args.Length;
        //    if (many ? (numCommandArgs == 0 || numCommandArgs % 7 != 0)
        //             : numCommandArgs != 7)
        //    {
        //        throw new ArgumentException("wrong number of arguments");
        //    }
        //    int nAclBindings = args.Length / 7;
        //    var aclBindings = new List<AclBinding>();
        //    for (int i = 0; i < nAclBindings; ++i)
        //    {
        //        var baseArg = i * 7;
        //        var resourceType = Enum.Parse<ResourceType>(args[baseArg]);
        //        var name = args[baseArg + 1];
        //        var resourcePatternType = Enum.Parse<ResourcePatternType>(args[baseArg + 2]);
        //        var principal = args[baseArg + 3];
        //        var host = args[baseArg + 4];
        //        var operation = Enum.Parse<AclOperation>(args[baseArg + 5]);
        //        var permissionType = Enum.Parse<AclPermissionType>(args[baseArg + 6]);

        //        if (name == "") { name = null; }
        //        if (principal == "") { principal = null; }
        //        if (host == "") { host = null; }

        //        aclBindings.Add(new AclBinding()
        //        {
        //            Pattern = new ResourcePattern
        //            {
        //                Type = resourceType,
        //                Name = name,
        //                ResourcePatternType = resourcePatternType
        //            },
        //            Entry = new AccessControlEntry
        //            {
        //                Principal = principal,
        //                Host = host,
        //                Operation = operation,
        //                PermissionType = permissionType
        //            }
        //        });
        //    }
        //    return aclBindings;
        //}




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
            //using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            //{
            //    try
            //    {
            //        var a = new TopicSpecification() { Name = topic, ReplicationFactor = 1, NumPartitions = num };
            //        await adminClient.CreateTopicsAsync(new TopicSpecification[] { a });
            //        Console.WriteLine("All create ACL operations completed successfully");
            //    }
            //    catch (CreatePartitionsException e)
            //    {
            //        Console.WriteLine("One or more create ACL operations failed.");
            //        for (int i = 0; i < e.Results.Count; ++i)
            //        {
            //            var result = e.Results[i];
            //            if (!result.Error.IsError)
            //            {
            //                Console.WriteLine($"Create ACLs operation {i} completed successfully");
            //            }
            //            else
            //            {
            //                Console.WriteLine($"An error occurred in create ACL operation {i}: Code: {result.Error.Code}" +
            //                $", Reason: {result.Error.Reason}");
            //            }
            //        }
            //    }
            //    catch (KafkaException e)
            //    {
            //        Console.WriteLine($"An error occurred calling the CreateAcls operation: {e.Message}");
            //    }
            //}
        }

        //static async Task DescribeAclsAsync(string bootstrapServers, string[] commandArgs)
        //{
        //    List<AclBindingFilter> aclBindingFilters;
        //    try
        //    {
        //        aclBindingFilters = ParseAclBindingFilters(commandArgs, false);
        //    }
        //    catch
        //    {
        //        Console.WriteLine("usage: .. <bootstrapServers> describe-acls <resource_type> <resource_name> <resource_patter_type> " +
        //            "<principal> <host> <operation> <permission_type>");
        //        Environment.ExitCode = 1;
        //        return;
        //    }

        //    using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
        //    {
        //        try
        //        {
        //            var result = await adminClient.DescribeAclsAsync(aclBindingFilters[0]);
        //            Console.WriteLine("Matching ACLs:");
        //            PrintAclBindings(result.AclBindings);
        //        }
        //        catch (DescribeAclsException e)
        //        {
        //            Console.WriteLine($"An error occurred in describe ACLs operation: Code: {e.Result.Error.Code}" +
        //                $", Reason: {e.Result.Error.Reason}");
        //        }
        //        catch (KafkaException e)
        //        {
        //            Console.WriteLine($"An error occurred calling the describe ACLs operation: {e.Message}");
        //        }
        //    }
        //}

        public static async Task DeleteAclsAsync(string[] topics)
        {

            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                try
                {

                    await adminClient.DeleteTopicsAsync(topics);
                    //int i = 0;
                    //foreach (var result in results)
                    //{
                    //    Console.WriteLine($"Deleted ACLs in operation {i}");
                    //    PrintAclBindings(result.AclBindings);
                    //    ++i;
                    //}
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

        public static void GetOffset()
        {

            //var _config = new ConsumerConfig
            //{
            //    BootstrapServers = Server,
            //    GroupId = GroupName,
            //    MaxPollIntervalMs = 70000,
            //    FetchWaitMaxMs = 50000,
            //    FetchMinBytes = 1,

            //    ApiVersionRequestTimeoutMs = 70000,
            //    EnableAutoCommit = true,
            //    EnableAutoOffsetStore = false,
            //    StatisticsIntervalMs = 5000,
            //    SessionTimeoutMs = 6000,
            //    //Latest 每次都从0开始 , Earliest 从最后位置开始拉取
            //    AutoOffsetReset = AutoOffsetReset.Earliest,
            //    EnablePartitionEof = true,
            //    MessageMaxBytes = 2048,
            //    // A good introduction to the CooperativeSticky assignor and incremental rebalancing:
            //    // https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb/
            //    //PartitionAssignmentStrategy = PartitionAssignmentStrategy.
            //};
        }
    }

}
