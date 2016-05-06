using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Xml;

namespace SQLParser
{
    class Program
    {
        private static SqlCommand command;
        private static HashSet<string> activeIds;

        public static string GetMD5Hash(string input)
        {
            MD5 md5 = MD5.Create();
            byte[] inputBytes = Encoding.ASCII.GetBytes(input);
            byte[] hash = md5.ComputeHash(inputBytes);
            var sb = new StringBuilder();
            for (int i = 0; i < hash.Length; i++)
            {
                sb.Append(hash[i].ToString("X2"));
            }
            return sb.ToString();
        }


        static void Main(string[] args)
        {
            Dictionary<string, string> files = new Dictionary<string, string>();
            Dictionary<string, string> fileInheritors = new Dictionary<string, string>();
            //SqlConnection connection = new SqlConnection();
            //connection.ConnectionString = "Server=kv-ws22;Database=MeetingsPlus;User ID=User;Password=Simga2016";
            //connection.Open();
            //command = connection.CreateCommand();

            string file = @"E:\temp\Blob.sql";
            string file2 = @"E:\temp\2.sql";

            if (File.Exists(file2))
                File.Open(file2, FileMode.Truncate).Close();

            using (TextReader reader = new StreamReader(file,Encoding.UTF8))
            {
                string buf;
                while ((buf = ReadQuery(reader)) != null)
                {
                    if (buf.Contains("INSERT"))
                    {
                        var data = ParseSql(buf);
                        //if (IsSearchble(data["[ID]"]))
                        //{
                        //    data["[IsSearchable]"] = "1";
                        //}

                        //allData.Add(data["[ID]"],data);

                        var fileData = GetMD5Hash(data["[Data]"]);
                        string blobId;
                        if (!files.TryGetValue(fileData, out blobId))
                        {
                            files.Add(fileData, data["[ID]"]);
                        }
                        else
                        {
                            data["[Data]"] = $"(select Data from Blob b where b.ID = {blobId})";
                            fileInheritors.Add(data["[ID]"], blobId);
                        }

                        WriteSql(data, file2);
                    }
                }

                //WriteLine(file2, "Main file \n\n\n");
                //WriteLine(file2, files);

                //WriteLine(file2, "Inheritor file: \n\n\n");
                //WriteLine(file2, fileInheritors);

            }
        }

        private static bool IsSecured(string xml)
        {
            if (string.IsNullOrEmpty(xml.Replace("NULL","")))
            {
                return true;
            }

            bool isSecured = false;
            XmlDocument doc = new XmlDocument();
            //doc.LoadXml("<d><p n=\"SortOrder\" t=\"System.Int32\"><v>0</v></p><p n=\"IsInternal\" t=\"System.Boolean\"><v>false</v></p><p n=\"File\" t=\"Mindroute.Core.Model.ContentRef\"><v>836/1</v></p><p n=\"DocumentVersion\" t=\"System.Int32\"><v>2</v></p><p n=\"Pul\" t=\"System.Int32\"><v>0</v></p><p n=\"Secrecy\" t=\"System.Int32\"><v>0</v></p><p n=\"EcmId\" t=\"System.String\"><v>456</v></p></d>");
            doc.LoadXml(xml.Replace("CONVERT(xml,N'","").Replace("',1)",""));

            foreach (XmlNode node in doc.DocumentElement.ChildNodes)
            {
                if (node.Attributes["n"].Value == "Pul")
                {
                    if (((XmlElement) node).InnerText == "1" || node.Attributes["n"].Value == "Secrecy")
                    {
                        isSecured = true;
                    }
                }
            }
            return isSecured;
        }


        private static string ReadQuery(TextReader reader)
        {
            var line = reader.ReadLine();
            if (line == null || !line.Contains("INSERT"))
            {
                return line;
            }
            line = line.Trim(' ',' ');
            if (!string.IsNullOrEmpty(line))
            {
                if (line[line.Length - 1] != ')')
                {
                    line += "\n";
                    while (true)
                    {
                        var buffer = reader.ReadLine();
                        if (string.IsNullOrEmpty(buffer))
                        {
                            line += "\n";
                            continue;
                        }

                        line = $"{line}{buffer}\n";
                        if (buffer[buffer.Length - 1] == ')')
                        {
                            break;
                        }
                        if (buffer.ToLower().Contains("insert"))
                        {
                            throw new AccessViolationException();
                        }
                    }
                }
            }
            return line?.Trim();
        }


        private static void WriteLine(string file, Dictionary<string, string> data)
        {
            using (TextWriter textWriter = new StreamWriter(file, true, Encoding.UTF8))
            {
                foreach (var item in data)
                {
                    textWriter.WriteLine($"{item.Key} -> {item.Value}");
                }
            }
        }

        private static void WriteLine(string file, string data)
        {
            using (TextWriter textWriter = new StreamWriter(file, true, Encoding.UTF8))
            {
                textWriter.WriteLine(data);
            }
        }

        private static void WriteSql(Dictionary<string, string> dictionary, string file)
        {
            bool isFirst = true;
            using (TextWriter textWriter = new StreamWriter(file, true, Encoding.UTF8))
            //using (TextWriter textWriter = new StringWriter())
            {
                textWriter.Write("INSERT INTO [dbo].[Blob] (");
                foreach (var item in dictionary)
                {
                    if (!isFirst)
                    {
                        textWriter.Write(", ");
                    }
                    isFirst = false;
                    textWriter.Write(item.Key);
                }
                textWriter.Write(") VALUES (");
                isFirst = true;
                foreach (var item in dictionary)
                {
                    if (!isFirst)
                    {
                        textWriter.Write(", ");
                    }
                    isFirst = false;
                    textWriter.Write(item.Value);
                }
                textWriter.Write(")");
                textWriter.Write(Environment.NewLine);

                //var sql = textWriter.ToString();
                //Add(sql); 
            }
        }


        private static Dictionary<string, string> ParseSql(string sql)
        {
            Dictionary<string, string> dictionary = new Dictionary<string, string>();
            var headStart = sql.IndexOf("(", 0, StringComparison.Ordinal);
            var headEnd = sql.IndexOf(")", headStart + 1, StringComparison.Ordinal);
            string head = sql.Substring(headStart + 1, headEnd - (headStart + 1));
            var heads = head.Split(',');
            foreach (var item in heads)
            {
                dictionary.Add(item.Trim(), string.Empty);
            }
            var valueStart = sql.IndexOf("(", headEnd + 1, StringComparison.Ordinal);
            var values = ParceValues(sql.Substring(valueStart + 1, sql.Length - (valueStart + 1)));

            for (int i = 0; i < values.Length; i++)
            {
                dictionary[heads[i].Trim()] = values[i];
            }

            return dictionary;
        }

        private static string[] ParceValues(string data)
        {
            List<string> resultList = new List<string>();
            int deep = 0;
            int deepString = 0;
            StringBuilder accum =new StringBuilder();
            char? ch = null;
            for (int i = 0; i < data.Length; i++)
            {
                var prev = ch;
                ch = data[i];

                if(deepString == 0)
                    if (ch == '(')
                        deep++;

                if (deepString == 0)
                    if (ch == ')')
                        deep--;

                if (ch == '\'' && prev == 'N')
                {
                    deepString++;
                    //deep++;
                }
                else if (ch == '\'' && prev == '\'')
                {
                    deepString++;
                    //deep++;
                }
                else if (ch == '\'' && deepString != 0)
                {
                    deepString--;
                    //deep--;
                }

                if (ch == ',' && deep == 0 && deepString == 0)
                {
                    resultList.Add(accum.ToString().Trim());
                    accum.Clear();
                    continue;
                }

                accum.Append(ch);
            }
            resultList.Add(accum.ToString().TrimEnd(')', ' ').Trim());
            return resultList.ToArray();
        }

        private static void Add(string sql)
        {
            command.CommandText = sql;
            command.ExecuteNonQuery();
        }

        private static bool IsSearchble(string id)
        {
            if (activeIds == null)
            {
                activeIds = new HashSet<string>();
                string sql = "select distinct t.ID as id from Translation t, Content c " +
                             " where t.id = c.ParentID " +
                             " and c.ContentTypeID in " +
                             " ('Formpipe.MeetingsPlus.Core.ContentTypes.AgendaItem', " +
                             " 'Formpipe.MeetingsPlus.Core.ContentTypes.Meeting', " +
                             " 'Formpipe.MeetingsPlus.Core.ContentTypes.Attachment', " +
                             " 'Formpipe.MeetingsPlus.Core.ContentTypes.Decision', " +
                             " 'Formpipe.MeetingsPlus.Core.ContentTypes.Paragraph', " +
                             " 'Formpipe.MeetingsPlus.Core.ContentTypes.MeetingGroupAttachment', " +
                             " 'Formpipe.MeetingsPlus.Core.ContentTypes.MeetingItemDescriptionAttachment') " +
                             "  Union " +
                             "  select t.ID " +
                             "   from Translation t, Content c " +
                             "   where c.ContentTypeID = 'Mindroute.Core.Model.Document' " +
                             "   and ContentData.value('(/d/p[@n = \"File\" and @t = \"Mindroute.Core.Model.ContentRef\"])[1]', 'nvarchar(max)') = CONCAT(c.ID, '/1') ";

                command.CommandText = sql;
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        activeIds.Add(reader[0].ToString());
                    }
                }
            }
            return activeIds.Contains(id);
        }


    }
}
