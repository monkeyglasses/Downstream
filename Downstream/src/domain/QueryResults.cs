using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Collections.Concurrent;
using com.bitscopic.downstream.domain.reporting;

namespace com.bitscopic.downstream.domain
{
    [Serializable]
    public class QueryResults
    {
        public QueryResults()
        {
            this.DdrResults = new List<DataTable>();
           // this.Report = new ThreadSafeReport();
        }

        IList<Exceptional> _exceptionals = new List<Exceptional>();
        public IList<Exceptional> Exceptionals { get { return _exceptionals; } set { _exceptionals = value; } }
        
        public IList<DataTable> DdrResults { get; set; }

        public String StringResult { get; set; }

        public Object Result { get; set; }

        /// <summary>
        /// Subfile IEN queues stored like this
        /// ["/45.06"] [[1],[5],[6],[15],[34]]
        /// ["/45.07"] [[2],[3],[5],[6],[31]]
        /// </summary>
        public Dictionary<String, IList<String>> SubQueryIens { get; set; }

        public IList<String> LabChemIens { get; set; }

        public bool ExtractionComplete { get; set; }

        Report _rpt = new Report("QueryResults Report");
        public Report Report { get { return _rpt; } }
    }
}
