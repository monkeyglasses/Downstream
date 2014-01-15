using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace com.bitscopic.downstream.domain
{
    [Serializable]
    public class Extractor
    {
        public string HostName { get; set; }
        public Int32 ListeningPort { get; set; }
        public string SiteCode { get; set; }
        public string VistaFile { get; set; }
        public DateTime Timestamp { get; set; }
        public ExtractorConfiguration Configuration { get; set; }
       // public string CurrentQueryString { get; set; }

        public Extractor(string hostname, Int32 port, string siteCode, string vistaFile, DateTime startTime)
        {
            HostName = hostname;
            ListeningPort = port;
            SiteCode = siteCode;
            VistaFile = vistaFile;
            Timestamp = startTime;
        }

        /// <summary>
        /// Make a string from internal properties
        /// </summary>
        /// <returns>String of Extractor object</returns>
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            
            sb.Append("Hostname:Port - ");
            sb.Append(HostName);
            sb.Append(":");
            sb.AppendLine(ListeningPort.ToString());
            sb.Append("Site Code: ");
            sb.AppendLine(SiteCode);
            sb.Append("Vista File: ");
            sb.AppendLine(VistaFile);
            sb.Append("Timestamp: ");
            sb.AppendLine(Timestamp.ToString());

            return sb.ToString();
        }
    }
}
