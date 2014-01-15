using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace com.bitscopic.downstream.domain.reporting
{
    [Serializable]
    public class ExtractorReport : Report
    {
        public ExtractorReport(String batchId) : base(batchId) { }

        IList<Exceptional> _exceptionals = new List<Exceptional>();
        public IList<Exceptional> Exceptionals { get { return _exceptionals; } set { _exceptionals = value; } }

        public Int32 RecordsExtracted { get; set; }

        public String StartIen { get; set; }

        public String LastIen { get; set; }

        /// <summary>
        /// The extractor's host name
        /// </summary>
        public string ExtractorHostName { get; set; }

        /// <summary>
        /// This corresponds to the field in the database that denotes whether an extractor successfully completed
        /// the job it received from the Orchestrator
        /// </summary>
        public bool Errored { get; set; }

        ExtractorConfiguration _config = new ExtractorConfiguration();
        /// <summary>
        /// The extractor's configuration class
        /// </summary>
        public ExtractorConfiguration Configuration { get { return _config; } }


        public void setConfiguration(ExtractorConfiguration config)
        {
            _config = config;
        }

        /// <summary>
        /// Build a string based on this class' info, error and debug messages and the exception strings
        /// </summary>
        /// <returns>A string representing the properties of this class</returns>
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();

            sb.AppendLine("Extractor Host Name: " + this.ExtractorHostName);

            if (_config != null)
            {
                sb.AppendLine("Extractor Configuration:");
                sb.Append(_config.ToString());
            }
            sb.Append(base.ToString());

            return sb.ToString();
        }
    }
}
