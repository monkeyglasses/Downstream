using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace com.bitscopic.downstream.domain
{
    public enum ServiceStatus
    {
        RUNNING,
        PAUSED,
        WAITING,
        STOPPED
    }

    public class ServiceState
    {
        public ServiceState() { }

        public ServiceStatus Status { get; set; }

        public String StatusSetBy { get; set; }

        public String PercentageComplete { get; set; }

        public DateTime NextRun { get; set; }

        /// <summary>
        /// Build a string of all internal properties
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();

            if (!String.IsNullOrEmpty(this.PercentageComplete))
            {
                sb.AppendLine("Percentage Complete: " + this.PercentageComplete);
            }
            sb.AppendLine("Service Status: " + Enum.GetName(typeof(ServiceStatus), this.Status));

            return sb.ToString();
        }
    }

    public class VistaServiceState : ServiceState
    {
        public String CurrentIEN { get; set; }

        public String LastVistaIEN { get; set; }

        public Int64 RecordsInFile { get; set; }

        public Int64 ParentRecordsExtracted { get; set; }

        public Int64 ChildRecordsExtracted { get; set; }

        /// <summary>
        /// Build a string of all internal properties
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();

            if (!String.IsNullOrEmpty(this.CurrentIEN))
            {
                sb.AppendLine("Current IEN: " + this.CurrentIEN);
            }
            if (!String.IsNullOrEmpty(this.LastVistaIEN))
            {
                sb.AppendLine("Last Vista IEN: " + this.LastVistaIEN);
            }
            if (this.ParentRecordsExtracted > 0)
            {
                sb.AppendLine("Top Level Records Extracted: " + this.ParentRecordsExtracted);
            }
            if (this.ChildRecordsExtracted > 0)
            {
                sb.AppendLine("Child Records Extracted: " + this.ChildRecordsExtracted);
            }
            sb.AppendLine(base.ToString());

            return sb.ToString();
        }
    }
}
