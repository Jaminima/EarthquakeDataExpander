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



        static void Main(string[] args)
        {
            CSVRow[] srcRows = File.ReadAllLines(srcCSV).Select(x => new CSVRow(x)).Skip(1).ToArray();

            List<string> dstRows = new List<string>();

            foreach (CSVRow row in srcRows)
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

                dstRows.Add(String.Join(',', newCells.Select(x => x.Replace(",", ""))));
            }

            File.WriteAllLines(dstCSV,dstRows);
        }
    }
}