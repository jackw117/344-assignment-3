using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Diagnostics;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using System.Configuration;
using System.IO;
using System.Xml.Linq;
using System.Xml;
using HtmlAgilityPack;
using Microsoft.WindowsAzure.Storage.Table;
using ClassLibrary1;

namespace WorkerRole1
{
    public class WorkerRole : RoleEntryPoint
    {
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent runCompleteEvent = new ManualResetEvent(false);

        //12,000 after about 25 minutes
        private HashSet<string> visitedUrls = new HashSet<string>();
        private List<string> cnnBlacklist = new List<string>();
        private List<string> bleacherBlacklist = new List<string>();
        private List<string> initialSitemap = new List<string>();
        private DateTime start = new DateTime(2016, 03, 01);
        private static XmlNamespaceManager mngr = new XmlNamespaceManager(new NameTable());
        private CloudQueue queue;
        private CloudTable table;


        public override void Run()
        {
            /*
            Trace.TraceInformation("WorkerRole1 is running");

            try
            {
                this.RunAsync(this.cancellationTokenSource.Token).Wait();
            }
            finally
            {
                this.runCompleteEvent.Set();
            }
            */
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(ConfigurationManager.AppSettings["StorageConnectionString"]);
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();
            queue = queueClient.GetQueueReference("urls");
            queue.CreateIfNotExists();

            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            table = tableClient.GetTableReference("pages");
            table.CreateIfNotExists();
            
            string cnnRobots = "http://www.cnn.com/robots.txt";
            string bleacherRobots = "http://bleacherreport.com/robots.txt";

            createBlacklist(cnnRobots, cnnBlacklist, initialSitemap);
            createBlacklist(bleacherRobots, bleacherBlacklist, initialSitemap);

            //while start is true
            while (true)
            {
                //getHTML("http://www.cnn.com/2016/05/10/asia/japan-artist-vagina-kayak/index.html");
                /*for (int i = 0; i < initialSitemap.Count; i++)
                {
                    searchSitemap(initialSitemap[i], queue);
                }*/
                CloudQueueMessage getMessage = queue.GetMessage();
                if (getMessage != null)
                {
                    string message = getMessage.AsString;
                    getHTML(message);
                }
            }
        }

        private void getHTML(string input)
        {
            HtmlDocument htmlDoc = new HtmlWeb().Load(input);
            if (htmlDoc.DocumentNode != null)
            {
                HtmlNode headNode = htmlDoc.DocumentNode.SelectSingleNode("//head");
                if (headNode != null)
                {
                    HtmlNode titleNode = headNode.SelectSingleNode("//title");
                    string title = titleNode.InnerText;
                    HtmlNodeCollection metaList = headNode.SelectNodes("//meta");
                    foreach (HtmlNode node in metaList)
                    {
                        if (node.GetAttributeValue("name", "not found") == "pubdate")
                        {
                            string date = node.Attributes["content"].Value;
                            addToTable(input, title, date);
                            break;
                        }
                    }
                }
                HtmlNode bodyNode = htmlDoc.DocumentNode.SelectSingleNode("//body");
                if (bodyNode != null)
                {
                    HtmlNodeCollection urlList = bodyNode.SelectNodes("//a");
                    foreach (HtmlNode node in urlList)
                    {
                        var href = node.Attributes["href"];
                        if (href != null)
                        {
                            string newUrl = node.Attributes["href"].Value;
                            if (newUrl.Contains(".cnn.com/"))
                            {
                                check(newUrl, "cnn");
                            } else if (newUrl.Contains("bleacherreport.com/"))
                            {
                                check(newUrl, "bleacher");
                            }
                        }
                    }
                }
            }
        }

        private void addToTable(string url, string title, string date)
        {
            TableOperation insertOperation = TableOperation.Insert(new Page(url, title, date));
            table.Execute(insertOperation);
        }
        
        private void searchSitemap(string url, CloudQueue queue)
        {
            XmlDocument doc = new XmlDocument();
            doc.Load(url);
            XmlNamespaceManager nsmgr = new XmlNamespaceManager(doc.NameTable);
            //maybe try automating this to make it better
            nsmgr.AddNamespace("sm", "http://www.sitemaps.org/schemas/sitemap/0.9");
            nsmgr.AddNamespace("img", "http://www.google.com/schemas/sitemap-image/1.1");
            nsmgr.AddNamespace("news", "http://www.google.com/schemas/sitemap-news/0.9");
            nsmgr.AddNamespace("video", "http://www.google.com/schemas/sitemap-video/1.1");
            XmlNode root = doc.DocumentElement;
            XmlNodeList sitemaps = root.SelectNodes("descendant::sm:sitemap", nsmgr);
            XmlNodeList urls = root.SelectNodes("descendant::sm:url", nsmgr);
            if (sitemaps.Count != 0)
            {
                foreach (XmlNode map in sitemaps)
                {
                    var urlnode = map.SelectSingleNode("sm:loc", nsmgr);
                    var modNode = map.SelectSingleNode("sm:lastmod", nsmgr);
                    string sitemapUrl = urlnode.InnerText;
                    DateTime modDate = DateTime.Parse(modNode.InnerText);
                    if (!initialSitemap.Contains(sitemapUrl) && modDate > start)
                    {
                        initialSitemap.Add(sitemapUrl);
                    }
                }
            } else if (urls.Count != 0)
            {
                foreach (XmlNode node in urls)
                {
                    var urlNode = node.SelectSingleNode("sm:loc", nsmgr);
                    string urlText = urlNode.InnerText;
                    if (urlText.Contains(".cnn.com/"))
                    {
                        check(urlText, "cnn");
                    } else
                    {
                        check(urlText, "bleacher");
                    }
                }
            }
            
            /*HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(url);
            request.UserAgent = "A .NET Web Crawler";
            WebResponse response = request.GetResponse();
            Stream stream = response.GetResponseStream();
            StreamReader reader = new StreamReader(stream);
            while (reader.EndOfStream == false)
            {
                string line = reader.ReadLine();
                string newLine = "";
                if (line.Contains(".cnn.com/"))
                {
                    newLine = check(line, queue, "cnn");
                } else if (line.Contains("bleacherreport.com/"))
                {
                    newLine = check(line, queue, "bleacher");
                }
                if (line.Contains("<sitemap>"))
                {
                    
                    if (doc.DocumentNode != null)
                    {
                        HtmlNode date = doc.DocumentNode.SelectSingleNode("//lastmod");
                        DateTime sitemapDate = DateTime.Parse(date.InnerText);
                        if (start < sitemapDate)
                        {
                            HtmlNode sitemapNode = doc.DocumentNode.SelectSingleNode("//loc");
                            string sitemapUrl = sitemapNode.InnerText;
                            initialSitemap.Add(sitemapUrl);
                        }
                    }
                    XElement xml = XElement.Parse(line);
                    XNamespace df = xml.Name.Namespace;
                    string strip = (string)xml.Element(df + "loc");
                    initialSitemap.Add(strip);
                } else if (line.Contains("<url>"))
                {
                    //<lastmod> as well
                    if (line.Contains("publication_date"))
                    {
                        //HtmlDocument doc = new HtmlWeb().Load(line);
                        if (doc.DocumentNode != null)
                        {
                            HtmlNode date = doc.DocumentNode.SelectSingleNode("//news:news");

                        }
                        XmlDocument xml = getXmlDocument(line);
                        XmlNodeList nodeList = xml.SelectNodes("//url");
                        foreach (XmlNode node in nodeList) // for each <testcase> node
                        {
                            var name = node.Attributes.GetNamedItem("loc").InnerText;
                            var date = node.Attributes.GetNamedItem("news:news").InnerText;
                        }
                                
                        //XNamespace df = xml.Name.Namespace;
                        //string strip = (string)xml.Element(df + "loc");
                        //DateTime current = DateTime.Parse(strip);
                        //if (current > start)
                        //{
                        //    check(line, queue, "cnn");
                        //}
                    } else
                    {
                        check(line, "cnn");
                    }
                }
            }*/
        }

        private string check(string line, string site)
        {
            //XDocument xml = XDocument.Parse(line);
            //string strip = xml.Root.Element("loc").Value;
            if (site == "cnn")
            {
                foreach (string url in cnnBlacklist)
                {
                    if (line.Contains(url))
                    {
                        return "";
                    }
                }
            } else if (site == "bleacher")
            {
                foreach (string url in bleacherBlacklist)
                {
                    if (line.Contains(url))
                    {
                        return "";
                    }
                }
            }
            if (!visitedUrls.Contains(line))
            {
                visitedUrls.Add(line);
                queue.AddMessage(new CloudQueueMessage(line));
                return line;
            } else
            {
                return "";
            }
        }

        private static void createBlacklist(string url, List<string> blacklist, List<string> sitemap)
        {
            HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(url);
            request.UserAgent = "A .NET Web Crawler";
            WebResponse response = request.GetResponse();
            Stream stream = response.GetResponseStream();
            StreamReader reader = new StreamReader(stream);
            while (reader.EndOfStream == false)
            {
                string line = reader.ReadLine();
                if (line.StartsWith("Disallow: "))
                {
                    string block = line.Substring(10);
                    blacklist.Add(block);
                } else if (line.StartsWith("Sitemap: "))
                {
                    string block = line.Substring(9);
                    if (url.Contains("bleacher"))
                    {
                        if (block.Contains("nba"))
                        {
                            sitemap.Add(block);
                        }
                    } else
                    {
                        sitemap.Add(block);
                    }
                }
            }
        }

        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections
            ServicePointManager.DefaultConnectionLimit = 12;

            // For information on handling configuration changes
            // see the MSDN topic at http://go.microsoft.com/fwlink/?LinkId=166357.

            bool result = base.OnStart();

            Trace.TraceInformation("WorkerRole1 has been started");

            return result;
        }

        public override void OnStop()
        {
            Trace.TraceInformation("WorkerRole1 is stopping");

            this.cancellationTokenSource.Cancel();
            this.runCompleteEvent.WaitOne();

            base.OnStop();

            Trace.TraceInformation("WorkerRole1 has stopped");
        }

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            // TODO: Replace the following with your own logic.
            while (!cancellationToken.IsCancellationRequested)
            {
                Trace.TraceInformation("Working");
                await Task.Delay(1000);
            }
        }
    }
}
