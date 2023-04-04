using ConfigHelper;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Drawing;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace AdminkafkaCli
{
    public partial class MainForm : Form
    {
        public MainForm()
        {
            InitializeComponent();
            AllocConsole();
        }

        #region 控制台
        const int STD_INPUT_HANDLE = -10;
        const uint ENABLE_QUICK_EDIT_MODE = 0x0040;
        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern IntPtr GetStdHandle(int hConsoleHandle);
        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern bool GetConsoleMode(IntPtr hConsoleHandle, out uint mode);
        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern bool SetConsoleMode(IntPtr hConsoleHandle, uint mode);
        [System.Runtime.InteropServices.DllImport("kernel32.dll", SetLastError = true)]
        [return: System.Runtime.InteropServices.MarshalAs(System.Runtime.InteropServices.UnmanagedType.Bool)]
        public static extern bool AllocConsole();

        [System.Runtime.InteropServices.DllImport("Kernel32")]
        public static extern void FreeConsole();//关闭控制台
        #endregion

        public int ShowOffsetLimit = 1000;

        private async void Form1_Load(object sender, EventArgs e)
        {
            ShowOffsetLimit = ConfigVals.ShowOffsetLimit;
            await CreateNodeAsync();
        }

        private async Task CreateNodeAsync()
        {
            try
            {
                //using var AdminClient = KafkaOpera.GetTopicMetadata;
                treeView1?.Nodes.Clear();
                var metadata = KafkaOpera.GetTopicMetadata();
                var listtop = metadata.Topics;
                var strings = listtop.Select(d => d.Topic).ToArray();
                TreeNode root = new TreeNode("Root");
                TreeNode topicNode = new TreeNode("Topics");
                treeView1.Nodes.Add(root);
                root.Nodes.Add(topicNode);
                for (int i = 0; i < listtop.Count; i++)
                {
                    TreeNode tbPar = new TreeNode();
                    tbPar.Text = "Partition";

                    TreeNode tn = new TreeNode();
                    tn.Text = listtop[i].Topic;
                    tn.Tag = listtop[i];
                    topicNode.Nodes.Add(tn);
                    tn.Nodes.Add(tbPar);

                    foreach (var item in listtop[i].Partitions)
                    {
                        TreeNode tn2 = new TreeNode();
                        tn2.Text = "Partition" + item.PartitionId;
                        tn2.Tag = item;
                        tbPar.Nodes.Add(tn2);
                    }
                }
                TreeNode bron = new TreeNode() { Text = "Brokers" };
                bron.ExpandAll();
                root.Nodes.Add(bron);

                metadata.Brokers.ForEach(d =>
                {
                    TreeNode bk = new TreeNode() { Text = $"[{d.BrokerId}] Host:{d.Host} Port:{d.Port} " };
                    bron.Nodes.Add(bk);
                }
                );
                try
                {
                    var group = await KafkaOpera.ListGroups();
                    if (group != null)
                    {
                        TreeNode tngroup = new TreeNode();
                        tngroup.Text = "Groups";
                        root.Nodes.Add(tngroup);
                        foreach (var item in group.Valid)
                        {
                            TreeNode groupNode = new TreeNode();
                            groupNode.Text = item.GroupId;
                            groupNode.Tag = item;
                            tngroup.Nodes.Add(groupNode);
                            var results = await KafkaOpera.AlterConsumerGroupOffsetsAsync(item.GroupId);

                            foreach (ListConsumerGroupOffsetsResult result in results)
                            {
                                List<string> listTopics = new List<string>();
                                TreeNode curNode = new TreeNode();
                                foreach (var partiMeta in result.Partitions.OrderByDescending(d => d.Topic))
                                {
                                    if (!listTopics.Contains(partiMeta.Topic))
                                    {
                                        TreeNode treeNode = new TreeNode() { Text = partiMeta.Topic };
                                        groupNode.Nodes.Add(treeNode);
                                        listTopics.Add(partiMeta.Topic);
                                        curNode = treeNode;
                                    }
                                    TreeNode pNode = new TreeNode();
                                    pNode.Text = $"Partition{partiMeta.Partition.Value}:{partiMeta.TopicPartitionOffset.Offset}";
                                    curNode.Nodes.Add(pNode);
                                }

                            }

                        }

                        root.Expand();
                        topicNode.Expand();
                        treeView1.AfterSelect += TreeView1_AfterSelect;
                        Text = "brootstrap: " + ConfigVals.BrootStrapServer;
                    }
                }
                catch (Exception ex)
                {

                    throw;
                }

            }
            catch (Exception)
            {

                throw;
            }
        }

        static string Getstring(int[] ints)
        {
            string str = "";
            for (int i = 0; i < ints.Length; i++)
            {
                if (i != ints.Length)
                {
                    str += ints[i] + ",";

                }
                else
                {
                    str += ints[i];
                }
            }
            return str;
        }

        private async void TreeView1_AfterSelect(object? sender, TreeViewEventArgs e)
        {
            if (sender != null)
            {
                var seletObj = (((TreeView)sender).SelectedNode.Tag);
                if (seletObj is TopicMetadata)
                {
                    TopicMetadata data = (TopicMetadata)seletObj;
                    List<KeyValuePair<string, string>> listShow = new List<KeyValuePair<string, string>>();
                    foreach (var item in data.Partitions)
                    {
                        Dictionary<string, string> dic = new Dictionary<string, string>();
                        dic.Add("PartitionId", "Partition" + item.PartitionId.ToString());
                        dic.Add("Leader", item.Leader.ToString());
                        dic.Add("Replicas", Getstring(item.Replicas));
                        dic.Add("InSyncReplicas", Getstring(item.InSyncReplicas));
                        dic.Add("Error", data.Error.ToString());
                        listShow.AddRange(dic.ToList());
                        listShow.Add(new KeyValuePair<string, string>("---", "---"));
                    }

                    dataGridView1.DataSource = listShow;
                }
                else if (seletObj is PartitionMetadata)
                {
                    PartitionMetadata topicMetadata = (PartitionMetadata)seletObj;
                    string topic = (((TreeView)sender).SelectedNode).Parent.Parent.Text;
                    DataTable dataTable = new DataTable();
                    string p = "Partition";
                    string of = "Offset";
                    string ts = "TimeSpan";
                    string tss = "DateTime";
                    dataTable.Columns.Add(p);
                    dataTable.Columns.Add(of);
                    dataTable.Columns.Add(ts);
                    dataTable.Columns.Add(tss);

                    CancellationTokenSource CancellationToken = new CancellationTokenSource();

                    ConsumerConfig _config = new ConsumerConfig
                    {
                        BootstrapServers = ConfigVals.BrootStrapServer,
                        GroupId = "AdminCli",
                    };

                    using (var consumer = new ConsumerBuilder<Ignore, byte[]>(_config).Build())
                    {
                        //return consumer.GetWatermarkOffsets(new TopicPartition(Topic, 0));
                        WatermarkOffsets watermarkOffsets = consumer.QueryWatermarkOffsets(new TopicPartition(topic, topicMetadata.PartitionId), new TimeSpan(0, 0, 1));
                        if (watermarkOffsets.High > 0)
                        {
                            if (watermarkOffsets.High > ShowOffsetLimit)
                            {
                                consumer.Assign(new TopicPartitionOffset(topic, topicMetadata.PartitionId, watermarkOffsets.High - ShowOffsetLimit));
                            }
                            else
                            {
                                consumer.Assign(new TopicPartitionOffset(topic, topicMetadata.PartitionId, watermarkOffsets.High));
                            }

                            try
                            {
                                int count = 0;
                                TimeZoneInfo localTime = TimeZoneInfo.Local;
                                while (true)
                                {
                                    try
                                    {
                                        var consumeResult = consumer.Consume(CancellationToken.Token);
                                        if (consumeResult.Message != null)
                                        {
                                            DataRow dr = dataTable.NewRow();
                                            dr[p] = consumeResult.Partition;
                                            dr[of] = consumeResult.Offset;
                                            dr[ts] = consumeResult.Message.Timestamp.UnixTimestampMs;
                                            dr[tss] = TimeZoneInfo.ConvertTimeFromUtc(consumeResult.Message.Timestamp.UtcDateTime, localTime).ToString("yyyy-MM-dd HH:mm:ss fff");
                                            dataTable.Rows.Add(dr);
                                            count++;
                                            if (count >= ShowOffsetLimit || consumeResult.IsPartitionEOF)
                                            {
                                                break;
                                            }
                                        }

                                    }
                                    catch (ConsumeException ex)
                                    {
                                        Console.WriteLine($"Consume error: {ex.Error.Reason}");
                                    }
                                }
                                dataGridView1.DataSource = dataTable;
                            }
                            catch (OperationCanceledException)
                            {
                                Console.WriteLine("Closing consumer.");
                                consumer.Close();
                            }
                        }
                    }
                }

            }

        }


        private async void btnDel_Click(object sender, EventArgs e)
        {
            if (treeView1.SelectedNode != null && treeView1.SelectedNode.Tag != null)
            {
                var seletObj = (treeView1.SelectedNode.Tag);
                if (seletObj is TopicMetadata)
                {
                    await KafkaOpera.DeleteAclsAsync(new string[] { ((TopicMetadata)seletObj).Topic });
                    CreateNodeAsync();
                    Console.WriteLine("删除topic");
                }
            }
        }

        private async void createTopicToolStripMenuItem_ClickAsync(object sender, EventArgs e)
        {
            CreateTopicForm f2 = new CreateTopicForm();
            if (f2.ShowDialog() == DialogResult.OK)
            {
                await CreateTopicAsync(f2.Topic, f2.Parti);
            }
        }

        public async Task CreateTopicAsync(string topic, int parti)
        {
            await KafkaOpera.CreateTopicAsync(topic, parti);
            Console.WriteLine("已创建topic");
            CreateNodeAsync();
            MessageBox.Show("已创建Topic");

        }

        private async void configToolStripMenuItem_Click(object sender, EventArgs e)
        {
            await Process.Start("notepad.exe ", ConfigVals.CONFIGFILE).WaitForExitAsync();
            ConfigVals.LoadFile();
            Form1_Load(null, null);
            //File.Open(ConfigVals.CONFIGFILE,FileMode.Open);
        }
    }
}
