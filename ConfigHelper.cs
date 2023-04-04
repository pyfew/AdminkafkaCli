using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace ConfigHelper
{
    public static class ConfigVals
    {
        public const string CONFIGFILE = "appconfig.json";
        static JObject Jobj;

        static ConfigVals()
        {
            LoadFile();
        }

        public static void LoadFile() {
            string fileconfig = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, CONFIGFILE);
            if (File.Exists(fileconfig))
            {
                string config = File.ReadAllText(fileconfig);
                Jobj = (JObject)JsonConvert.DeserializeObject(config);
            }
            else
            {
                Console.WriteLine("读取文件失败");
                Application.Exit();
            }
        }

        public static string BrootStrapServer => Jobj["BrootStrapServer"].ToString();

        public static int ShowOffsetLimit => int.Parse(Jobj["ShowOffsetLimit"].ToString());
    }
}
