using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace com.bitscopic.downstream.domain.reporting
{
    [Serializable]
    public class OrchestratorReport : Report
    {
        public OrchestratorReport(String batchId) : base(batchId) { HasError = "F"; }

        public string OrchestratorHostName { get; set; }

        public string HasError { get; set; }

        /// <summary>
        /// Build a string based on this class' info, error and debug messages and the exception strings
        /// </summary>
        /// <returns>A string representing the properties of this class</returns>
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();

            sb.AppendLine("Orchestrator Report (" + StartTimestamp.ToString() + " - " + DateTime.Now.ToString() + ")");
            sb.Append(base.ToString());

            return sb.ToString();
        }

        public new void clear()
        {
            this.HasError = "F";
            base.clear();
        }
    }
}
