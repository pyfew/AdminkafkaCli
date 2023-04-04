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
        const string CONFIG = "appconfig.json";
        static readonly JObject Jobj;

        static ConfigVals()
        {
            string fileconfig = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, CONFIG);
            if (File.Exists(fileconfig))
            {

                string config = File.ReadAllText(fileconfig);
                Jobj = (JObject)JsonConvert.DeserializeObject(config);
            }
            else
            {
                Console.WriteLine("读取文件失败");
            }
        }

        public static string BrootStrapServer => Jobj["BrootStrapServer"].ToString();
    }
}
