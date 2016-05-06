using System;
using System.Collections.Generic;
using System.IO;

namespace SQLParser
{
    public class InsertItem
    {
        public string Table { get; set; }

        public Dictionary<string, string> Parameters { get; set; }

        public string ToSql()
        {
            bool isFirst = true;
            using (TextWriter textWriter = new StringWriter())
            {
                textWriter.Write($"INSERT INTO {Table} (");
                foreach (var item in Parameters)
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
                foreach (var item in Parameters)
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

                return textWriter.ToString();
            }
        }
    }
}
