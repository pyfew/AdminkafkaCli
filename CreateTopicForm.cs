using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace AdminkafkaCli
{
    public partial class CreateTopicForm : Form
    {
        public CreateTopicForm()
        {
            InitializeComponent();
        }

        public int Parti { get; set; }

        public string Topic { get; set; }

        private void button1_Click(object sender, EventArgs e)
        {
            int outt = 0;
            if (int.TryParse(textBox2.Text, out outt) && !string.IsNullOrEmpty(textBox1.Text))
            {
                Topic = textBox1.Text;
                Parti = outt;
                this.Close();
            }

        }

        private void button2_Click(object sender, EventArgs e)
        {
            this.Close();
        }
    }
}
