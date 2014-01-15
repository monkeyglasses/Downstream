using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace com.bitscopic.downstream.domain.svc
{
    [Serializable]
    public class ExtractorTO : AbstractTO
    {
        public ExtractorConfigurationTO configuration;
        public String hostname;
        public Int32 listeningPort;
        public String siteCode;
        public String timestamp;
        public String vistaFile;

        public ExtractorTO() { }

        public ExtractorTO(Extractor extractor)
        {
            if (extractor == null)
            {
                return;
            }
            if (extractor.Configuration != null)
            {
                this.configuration = new ExtractorConfigurationTO(extractor.Configuration);
            }

            this.hostname = extractor.HostName;
            this.listeningPort = extractor.ListeningPort;
            this.siteCode = extractor.SiteCode;
            this.timestamp = extractor.Timestamp.ToString();
            this.vistaFile = extractor.VistaFile;
        }
    }

    #region Lite Messaging

    [Serializable]
    public class ExtractorTOLite : AbstractTO
    {
        public String hostname;
        public Int32 listeningPort;
        public String siteCode;
        public String timestamp;
        public String vistaFile;

        public ExtractorTOLite() { }

        public ExtractorTOLite(Extractor extractor)
        {
            if (extractor == null)
            {
                return;
            }

            this.hostname = extractor.HostName;
            this.listeningPort = extractor.ListeningPort;
            this.siteCode = extractor.SiteCode;
            this.timestamp = extractor.Timestamp.ToString();
            this.vistaFile = extractor.VistaFile;
        }
    }

    #endregion
}
