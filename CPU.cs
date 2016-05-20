using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClassLibrary1
{
    public class CPU : TableEntity
    {
        public string cpu { get; set; }

        public CPU() { }

        public CPU(string val)
        {
            this.cpu = val;

            this.PartitionKey = "cpu";
            this.RowKey = "cpu";
        }
    }
}
