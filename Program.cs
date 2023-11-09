using System.Collections.Concurrent;
using System.Net;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace EarthquakeDataExpander
{
    internal class CSVRow
    {
        private string rowStr;
        private int idx = 0;

        public CSVRow(string rowStr) { 
            this.rowStr = rowStr;
        }

        public string nextCell()
        {
            string cell = "";
            int start = idx;
            bool text = false;

            int i = start;

            for (; i < rowStr.Length; i++)
            {
                var c = rowStr[i];

                if (c == '\"')
                {
                    text = !text;
                }
                else if (c == ',' && !text)
                {
                    break;
                }
                else { 
                    cell += c;
                }
            }

            idx = i + 1;
            return cell;
        }
    }

    internal class Program
    {
        static string srcCSV = "C:/Users/oscar/Downloads/QuakesToday.csv";
        static string dstCSV = "C:/Users/oscar/Downloads/Quakes.csv";

        static HttpClient httpClient = new HttpClient();

        static ConcurrentBag<string> dstRows = new ConcurrentBag<string>();

        static int done = 0;

        static string[] GetDataFromLink(string link)
        {
            using (var req = httpClient.GetAsync(link))
            {
                req.Wait();
                var res = req.Result;

                var getBody = res.Content.ReadAsStringAsync();
                getBody.Wait();
                var body = getBody.Result;

                if (res.StatusCode == HttpStatusCode.OK)
                {
                    var linkPreText = "https://earthquake.usgs.gov/earthquakes/eventpage/";

                    var findLink = body.IndexOf(linkPreText);

                    var getLink = body.Substring(findLink);
                    var findLinkEnd = getLink.IndexOf("\"");

                    getLink = getLink.Substring(linkPreText.Length, findLinkEnd - linkPreText.Length);

                    using (var reqDetails = httpClient.GetAsync($"https://earthquake.usgs.gov/fdsnws/event/1/query?eventid={getLink}&format=geojson"))
                    {
                        reqDetails.Wait();
                        var resDetails = reqDetails.Result;

                        if (resDetails.StatusCode == HttpStatusCode.OK)
                        {
                            var getDetailsBody = resDetails.Content.ReadAsStringAsync();
                            getDetailsBody.Wait();
                            var detailsBody = getDetailsBody.Result;

                            var detailsJson = JsonSerializer.Deserialize<JsonElement>(detailsBody);

                            var coordinates = detailsJson.GetProperty("geometry").GetProperty("coordinates");

                            return new string[] { coordinates[0].GetDouble().ToString(), coordinates[1].GetDouble().ToString(), coordinates[2].GetDouble().ToString() };
                        }
                    }
                }
                else if (res.StatusCode == HttpStatusCode.Moved)
                {
                    string lnk = body.Substring(body.IndexOf("http")+5);
                    lnk = "https:" + lnk.Substring(0, lnk.IndexOf("\""));
                    return GetDataFromLink(lnk);
                }

            }
            return new string[] { };
        }

        static void ProcessRow(CSVRow row)
        {
            var date = row.nextCell();
            var tweetId = row.nextCell();
            var text = row.nextCell();
            var username = row.nextCell();

            var magnitude = float.TryParse(text.Substring(0, 3), out var _mag) ? _mag : -1;
            var dist = float.TryParse(text.Substring(27, 3).Split(' ')[0], out var _dist) ? _dist : -1;

            var location = dist == -1 ? text.Substring(27, text.Length - 23 - 27) : text.Substring(29, text.Length - 29 - 24);
            var locationParts = location.Trim().Split(" ");

            var where = "";


            if (locationParts.Length == 1)
            {
                where = location;
            }
            else if (new[] { "north", "south", "east", "west" }.Any(x => locationParts[0].ToLower().StartsWith(x)))
            {
                where = location;
            }
            else if (locationParts[1] == "from")
            {
                where = String.Join(" ", locationParts.Skip(2));
            }
            else if (locationParts[1] == "of")
            {
                where = String.Join(" ", locationParts.Skip(1));
            }
            else if (locationParts.Length <= 2)
            {
                where = location;
            }
            else if (locationParts[2] == "of")
            {
                where = String.Join(" ", locationParts.Skip(3));
            }
            else if (locationParts[2] == "from")
            {
                where = String.Join(" ", locationParts.Skip(3));
            }
            else if (locationParts.Length <= 3)
            {
                where = location;
            }
            else if (locationParts[3] == "of")
            {
                where = String.Join(" ", locationParts.Skip(4));
            }

            var link = text.Substring(text.Length - 23);

            var newCells = new string[] { date, tweetId, text, username, magnitude.ToString(), dist.ToString(), where, link };

            var r = String.Join(',', newCells.Concat(GetDataFromLink(link)).Select(x => x.Replace(",", "")));

            dstRows.Add(r);

            done++;

            return;
        }

        static void Main(string[] args)
        {
            CSVRow[] srcRows = File.ReadAllLines(srcCSV).Select(x => new CSVRow(x)).Skip(1).ToArray();

            int steps = 500;

            for (int i = 0; i < srcRows.Length / steps; i++)
            {
                var tRows = srcRows.Skip(i*steps).Take(steps).Select(x => new Thread(y => ProcessRow(x))).ToArray();

                foreach (Thread t in tRows)
                {
                    t.Start();
                }

                while (tRows.Any(x => x.IsAlive))
                {
                    Console.WriteLine($"Batch {i} - Finished {done}/{srcRows.Length}\r");
                    Thread.Sleep(100);
                }
            }

            File.WriteAllLines(dstCSV,dstRows);
        }
    }
}