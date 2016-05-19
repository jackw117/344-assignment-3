using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace ClassLibrary1
{
    public class Counter : TableEntity
    {
        public int count { get; set; }

        public Counter() { }

        public Counter(int previous)
        {
            this.count = previous + 1;

            this.PartitionKey = "counter";
            this.RowKey = "counter";
        }
    }
}
