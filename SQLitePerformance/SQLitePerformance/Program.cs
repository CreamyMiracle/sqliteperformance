using SQLite;
using SQLiteNetExtensions.Extensions;
using SQLitePerformance.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Common;

namespace SQLitePerformance
{
    internal class Program
    {
        private static int _databaseSize = 20000;
        private static int _operationCount = 500;
        private static int _currId = 0;
        private static List<string> _pragmas = new List<string>()
        {
            //"PRAGMA journal_mode = OFF",
            ////"PRAGMA journal_mode = MEMORY",
            ////"PRAGMA journal_mode = WAL",
            //"PRAGMA synchronous = OFF",
            ////"PRAGMA synchronous = NORMAL",
            //"PRAGMA locking_mode = EXCLUSIVE",
            ////"PRAGMA locking_mode = NORMAL"
        };

        private static ConcurrentDictionary<string, KeyValuePair<TimeSpan, List<string>>> _fastestOperation = new ConcurrentDictionary<string, KeyValuePair<TimeSpan, List<string>>>();

        private static List<Func<SQLiteConnection, int, TimeSpan>> _funcs = new List<Func<SQLiteConnection, int, TimeSpan>>()
        {
            Insert,
            InsertInTransaction,

            InsertAll,
            InsertAllInTransaction,

            InsertOrReplace,
            InsertOrReplaceInTransaction,

            InsertOrReplaceAllWithChildren,
            InsertOrReplaceAllWithChildrenInTransaction,
        };

        private static TimeSpan Run(SQLiteConnection con, int count, Func<SQLiteConnection, int, TimeSpan> func)
        {
            return func(con, count);
        }

        static void Main(string[] args)
        {
            IEnumerable<List<string>> pragmaCombinations = GetCombinations(_pragmas);

            foreach (var currPragmas in pragmaCombinations)
            {
                var path = Path.Combine(Directory.GetCurrentDirectory(), Guid.NewGuid().ToString() + ".db");
                SQLiteConnection con = new SQLiteConnection(path);
                foreach (string pragma in currPragmas)
                {
                    con.ExecuteScalar<string>(pragma);
                }

                foreach (Func<SQLiteConnection, int, TimeSpan> func in _funcs)
                {
                    string methodName = func.Method.Name.PadRight(50);
                    ResetDatabase(con, _databaseSize);
                    TimeSpan duration = func(con, _operationCount);
                    Console.WriteLine(methodName + duration);
                    CheckFastestOperation(methodName, currPragmas, duration);
                }

                con.Close();
            }

            Console.WriteLine(Environment.NewLine);
            List<string> keys = _fastestOperation.Keys.ToList();
            keys.Sort();
            Console.WriteLine("Fastest operations:");
            foreach (var key in keys)
            {
                _fastestOperation.TryGetValue(key, out var value);
                Console.WriteLine("Operation: " + key + "   Duration: " + value.Key.TotalMilliseconds);
                List<string> sortedPragmas = value.Value;
                sortedPragmas.Sort();
                foreach (string pragma in sortedPragmas)
                {
                    Console.WriteLine("     Pragma: " + pragma);
                }
                Console.WriteLine(Environment.NewLine);
            }
        }

        private static void CheckFastestOperation(string operation, List<string> pragmas, TimeSpan duration)
        {
            if (_fastestOperation.TryGetValue(operation, out KeyValuePair<TimeSpan, List<string>> value))
            {
                if (value.Key > duration)
                {
                    _fastestOperation[operation] = new KeyValuePair<TimeSpan, List<string>>(duration, new List<string>(pragmas));
                }
            }
            else
            {
                _fastestOperation.TryAdd(operation, new KeyValuePair<TimeSpan, List<string>>(duration, new List<string>(pragmas)));
            }
        }

        #region Functions
        private static TimeSpan ResetDatabase(SQLiteConnection con, int count)
        {
            DateTime start = DateTime.Now;
            con.DropTable<Record>();
            con.CreateTable<Record>();
            List<Record> records = new List<Record>();

            _currId = 0;
            for (int i = _currId; i < count; i++)
            {
                records.Add(new Record { Id = i, Name = i.ToString() });
            }

            con.InsertAll(records, runInTransaction: true);

            DateTime end = DateTime.Now;
            return end - start;
        }

        private static TimeSpan SearchWithTable(SQLiteConnection con, int count)
        {
            Random rnd = new Random();
            List<int> ids = new List<int>();

            for (int i = 0; i < count; i++)
            {
                ids.Add(rnd.Next(0, _databaseSize));
            }

            DateTime start = DateTime.Now;
            con.Table<Record>().Where(r => ids.Contains(r.Id)).ToList();
            DateTime end = DateTime.Now;
            return end - start;
        }

        private static TimeSpan SearchWithTableInTransaction(SQLiteConnection con, int count)
        {
            Random rnd = new Random();
            List<int> ids = new List<int>();

            for (int i = 0; i < count; i++)
            {
                ids.Add(rnd.Next(0, _databaseSize));
            }

            DateTime start = DateTime.Now;
            con.RunInTransaction(() =>
            {
                con.Table<Record>().Where(r => ids.Contains(r.Id)).ToList();
            });
            DateTime end = DateTime.Now;
            return end - start;
        }

        private static TimeSpan SearchWithQuery(SQLiteConnection con, int count)
        {
            Random rnd = new Random();

            DateTime start = DateTime.Now;
            for (int i = 0; i < count; i++)
            {
                con.Query<Record>("SELECT * FROM Record WHERE Id = ?", i);
            }
            DateTime end = DateTime.Now;
            return end - start;
        }

        private static TimeSpan SearchWithQueryInTransaction(SQLiteConnection con, int count)
        {
            Random rnd = new Random();

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

        private static TimeSpan SearchInLoop(SQLiteConnection con, int count)
        {
            Random rnd = new Random();

            DateTime start = DateTime.Now;
            for (int i = 0; i < count; i++)
            {
                int rndId = rnd.Next(0, _databaseSize);
                con.Find<Record>(rndId);
            }
            DateTime end = DateTime.Now;
            return end - start;
        }

        private static TimeSpan SearchInLoopInTransaction(SQLiteConnection con, int count)
        {
            Random rnd = new Random();

            DateTime start = DateTime.Now;
            con.RunInTransaction(() =>
            {
                for (int i = 0; i < count; i++)
                {
                    int rndId = rnd.Next(0, _databaseSize);
                    con.Find<Record>(rndId);
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

        private static TimeSpan InsertInTransaction(SQLiteConnection con, int count)
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

        private static TimeSpan InsertOrReplaceInTransaction(SQLiteConnection con, int count)
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

        private static TimeSpan InsertOrReplaceAllWithChildrenInTransaction(SQLiteConnection con, int count)
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

        private static TimeSpan InsertAllInTransaction(SQLiteConnection con, int count)
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
