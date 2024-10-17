using SQLite;
using SQLiteNetExtensions.Extensions;
using SQLitePerformance.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Reflection;

namespace SQLitePerformance
{
    internal class Program
    {
        private static int _initSize = 10000;
        private static int _operationCount = 100;
        private static int _currId = 0;
        private static List<string> _pragmas = new List<string>()
        {
            "PRAGMA journal_mode = OFF",
            //"PRAGMA journal_mode = MEMORY",
            //"PRAGMA journal_mode = WAL",

            "PRAGMA synchronous = OFF",
            //"PRAGMA synchronous = NORMAL",

            "PRAGMA locking_mode = EXCLUSIVE",
            //"PRAGMA locking_mode = NORMAL"
        };


        static void Main(string[] args)
        {

            List<List<string>> pragmaCombinations = GetCombinations(_pragmas).ToList();

            foreach (var currPragmas in pragmaCombinations)
            {
                var path = Path.Combine(Directory.GetCurrentDirectory(), Guid.NewGuid().ToString() + ".db");
                Console.WriteLine(path);
                Console.WriteLine(Environment.NewLine);
                SQLiteConnection con = new SQLiteConnection(path);
                foreach (string pragma in currPragmas)
                {
                    Console.WriteLine(pragma);
                    con.ExecuteScalar<string>(pragma);
                }

                bool logInits = false;

                LogStats(InitDatabase, con, logInits);
                LogStats(SearchTable, con);

                LogStats(InitDatabase, con, logInits);
                LogStats(TX_SearchTable, con);

                LogStats(InitDatabase, con, logInits);
                LogStats(SearchQuery, con);

                LogStats(InitDatabase, con, logInits);
                LogStats(TX_SearchQuery, con);

                LogStats(InitDatabase, con, logInits);
                LogStats(FindLoop, con);

                LogStats(InitDatabase, con, logInits);
                LogStats(TX_FindLoop, con);

                LogStats(InitDatabase, con, logInits);
                LogStats(Insert, con);

                LogStats(InitDatabase, con, logInits);
                LogStats(TX_Insert, con);

                LogStats(InitDatabase, con, logInits);
                LogStats(InsertOrReplace, con);

                LogStats(InitDatabase, con, logInits);
                LogStats(TX_InsertOrReplace, con);

                LogStats(InitDatabase, con, logInits);
                LogStats(InsertOrReplaceAllWithChildren, con);

                LogStats(InitDatabase, con, logInits);
                LogStats(TX_InsertOrReplaceAllWithChildren, con);

                LogStats(InitDatabase, con, logInits);
                LogStats(InsertAll, con);

                LogStats(InitDatabase, con, logInits);
                LogStats(TX_InsertAll, con);

                con.Close();
            }
        }

        private static double ConvertBytesToMegabytes(long bytes)
        {
            return (bytes / 1024f) / 1024f;
        }

        private static void LogStats(Func<SQLiteConnection, int, TimeSpan> func, SQLiteConnection con, bool log = true)
        {
            string methodName = func.Method.Name.PadRight(40);
            string msg = "";
            string size = "";
            try
            {
                TimeSpan duration = func(con, _operationCount);
                msg = duration.TotalMilliseconds.ToString().PadRight(20);
                size = "Bytes: " + Math.Round(ConvertBytesToMegabytes(new System.IO.FileInfo(con.DatabasePath).Length), 2).ToString() + "MB";
            }
            catch (Exception ex)
            {
                msg = "ERROR";
            }
            if (log)
            {
                Console.WriteLine(methodName + msg + size);
            }
        }

        #region Functions
        private static TimeSpan InitDatabase(SQLiteConnection con, int useless)
        {
            DateTime start = DateTime.Now;
            con.DropTable<Record>();
            con.CreateTable<Record>();
            List<Record> records = new List<Record>();

            _currId = 0;
            for (int i = _currId; i < _initSize; i++)
            {
                records.Add(new Record { Id = i, Name = i.ToString(), Prop1 = i.ToString(), Prop2 = i.ToString(), Prop3 = i.ToString(), Prop4 = i.ToString() });
            }
            _currId = _initSize;
            con.InsertAll(records, runInTransaction: true);

            DateTime end = DateTime.Now;
            return end - start;
        }

        private static TimeSpan SearchTable(SQLiteConnection con, int count)
        {
            List<int> ids = new List<int>();

            for (int i = 0; i < count; i++)
            {
                ids.Add(i);
            }

            DateTime start = DateTime.Now;
            con.Table<Record>().Where(r => ids.Contains(r.Id)).ToList();
            DateTime end = DateTime.Now;
            return end - start;
        }

        private static TimeSpan TX_SearchTable(SQLiteConnection con, int count)
        {
            List<int> ids = new List<int>();

            for (int i = 0; i < count; i++)
            {
                ids.Add(i);
            }

            DateTime start = DateTime.Now;
            con.RunInTransaction(() =>
            {
                con.Table<Record>().Where(r => ids.Contains(r.Id)).ToList();
            });
            DateTime end = DateTime.Now;
            return end - start;
        }

        private static TimeSpan SearchQuery(SQLiteConnection con, int count)
        {
            DateTime start = DateTime.Now;
            for (int i = 0; i < count; i++)
            {
                con.Query<Record>("SELECT * FROM Record WHERE Id = ?", i);
            }
            DateTime end = DateTime.Now;
            return end - start;
        }

        private static TimeSpan TX_SearchQuery(SQLiteConnection con, int count)
        {
            DateTime start = DateTime.Now;
            con.RunInTransaction(() =>
            {
                for (int i = 0; i < count; i++)
                {
                    con.Query<Record>("SELECT * FROM Record WHERE Id = ?", i);
                }
            });
            DateTime end = DateTime.Now;
            return end - start;
        }

        private static TimeSpan FindLoop(SQLiteConnection con, int count)
        {
            DateTime start = DateTime.Now;
            for (int i = 0; i < count; i++)
            {
                con.Find<Record>(i);
            }
            DateTime end = DateTime.Now;
            return end - start;
        }

        private static TimeSpan TX_FindLoop(SQLiteConnection con, int count)
        {
            DateTime start = DateTime.Now;
            con.RunInTransaction(() =>
            {
                for (int i = 0; i < count; i++)
                {
                    con.Find<Record>(i);
                }
            });
            DateTime end = DateTime.Now;
            return end - start;
        }

        private static TimeSpan Insert(SQLiteConnection con, int count)
        {
            List<Record> records = new List<Record>();
            for (int i = _currId; i < _currId + count; i++)
            {
                records.Add(new Record { Id = i, Name = i.ToString() });
            }

            DateTime start = DateTime.Now;
            foreach (Record rec in records)
            {
                con.Insert(rec);
            }
            DateTime end = DateTime.Now;

            return end - start;
        }

        private static TimeSpan TX_Insert(SQLiteConnection con, int count)
        {
            List<Record> records = new List<Record>();
            for (int i = _currId; i < _currId + count; i++)
            {
                records.Add(new Record { Id = i, Name = i.ToString() });
            }

            DateTime start = DateTime.Now;
            con.RunInTransaction(() =>
            {
                foreach (Record rec in records)
                {
                    con.Insert(rec);
                }
            });
            DateTime end = DateTime.Now;

            return end - start;
        }

        private static TimeSpan InsertOrReplace(SQLiteConnection con, int count)
        {
            List<Record> records = new List<Record>();
            for (int i = _currId; i < _currId + count; i++)
            {
                records.Add(new Record { Id = i, Name = i.ToString() });
            }

            DateTime start = DateTime.Now;
            foreach (Record rec in records)
            {
                con.InsertOrReplace(rec);
            }
            DateTime end = DateTime.Now;

            return end - start;
        }

        private static TimeSpan TX_InsertOrReplace(SQLiteConnection con, int count)
        {
            List<Record> records = new List<Record>();
            for (int i = _currId; i < _currId + count; i++)
            {
                records.Add(new Record { Id = i, Name = i.ToString() });
            }

            DateTime start = DateTime.Now;
            con.RunInTransaction(() =>
            {
                foreach (Record rec in records)
                {
                    con.InsertOrReplace(rec);
                }
            });
            DateTime end = DateTime.Now;

            return end - start;
        }

        private static TimeSpan InsertOrReplaceAllWithChildren(SQLiteConnection con, int count)
        {
            List<Record> records = new List<Record>();
            for (int i = _currId; i < _currId + count; i++)
            {
                records.Add(new Record { Id = i, Name = i.ToString() });
            }

            DateTime start = DateTime.Now;
            con.InsertOrReplaceAllWithChildren(records);
            DateTime end = DateTime.Now;

            return end - start;
        }

        private static TimeSpan TX_InsertOrReplaceAllWithChildren(SQLiteConnection con, int count)
        {
            List<Record> records = new List<Record>();
            for (int i = _currId; i < _currId + count; i++)
            {
                records.Add(new Record { Id = i, Name = i.ToString() });
            }

            DateTime start = DateTime.Now;
            con.RunInTransaction(() =>
            {
                con.InsertOrReplaceAllWithChildren(records);
            });
            DateTime end = DateTime.Now;

            return end - start;
        }

        private static TimeSpan InsertAll(SQLiteConnection con, int count)
        {
            List<Record> records = new List<Record>();
            for (int i = _currId; i < _currId + count; i++)
            {
                records.Add(new Record { Id = i, Name = i.ToString() });
            }

            DateTime start = DateTime.Now;
            con.InsertAll(records, runInTransaction: false);
            DateTime end = DateTime.Now;

            return end - start;
        }

        private static TimeSpan TX_InsertAll(SQLiteConnection con, int count)
        {
            List<Record> records = new List<Record>();
            for (int i = _currId; i < _currId + count; i++)
            {
                records.Add(new Record { Id = i, Name = i.ToString() });
            }

            DateTime start = DateTime.Now;
            con.InsertAll(records, runInTransaction: true);
            DateTime end = DateTime.Now;

            return end - start;
        }

        #endregion

        public static IEnumerable<List<T>> GetCombinations<T>(List<T> source)
        {
            for (var i = 0; i < (1 << source.Count); i++)
                yield return source
                   .Where((t, j) => (i & (1 << j)) != 0)
                   .ToList();
        }
    }
}
