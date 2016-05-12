using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using System.Text.RegularExpressions;

namespace ClassLibrary1
{
    public class Page : TableEntity
    {
        public string urlName { get; private set; }
        public string title { get; private set; }
        public string date { get; private set; }

        public Page() { }

        public Page(string url, string title, string date)
        {
            this.urlName = url;
            this.title = title;
            this.date = date;
            
            this.PartitionKey = Regex.Replace(title.ToLower(), @"[^0-9a-z]+", " ");
            this.RowKey = Guid.NewGuid().ToString();
        }
    }
}
