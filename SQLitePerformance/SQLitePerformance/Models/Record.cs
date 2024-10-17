using SQLite;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLitePerformance.Models
{
    internal class Record
    {
        [PrimaryKey]
        public int Id { get; set; }

        [Indexed(Order = 1, Unique = true)]
        public string Name { get; set; }

        [Indexed(Order = 2, Unique = true)]
        public string Prop1 { get; set; }

        [Indexed(Order = 3, Unique = true)]
        public string Prop2 { get; set; }

        [Indexed(Order = 4, Unique = true)]
        public string Prop3 { get; set; }

        [Indexed(Order = 5, Unique = true)]
        public string Prop4 { get; set; }
    }
}
