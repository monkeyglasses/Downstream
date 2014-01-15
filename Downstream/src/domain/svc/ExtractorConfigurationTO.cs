using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using com.bitscopic.downstream.service;

namespace com.bitscopic.downstream.domain.svc
{
    [Serializable]
    public class ExtractorConfigurationTO
    {
        public String batchId;
        public String extractorMode;
        public string maxRecordsPerQuery;
        public string cron;
        public string siteCode;
        public string sites;
        public string startIen;
        public QueryConfigurationTreeTO queryConfigurations;

        public ExtractorConfigurationTO() { }

        public ExtractorConfigurationTO(ExtractorConfiguration config)
        {
            // constructor should instantiate root node, child nodes consumable by any web service client
            this.queryConfigurations = new QueryConfigurationTreeTO(config.QueryConfigurations);

            this.batchId = config.BatchId;
            this.cron = config.CRON;
            this.extractorMode = Enum.GetName(typeof(ExtractorMode), config.ExtractMode);
            this.maxRecordsPerQuery = config.MaxRecordsPerQuery;
            this.siteCode = config.SiteCode;
            this.sites = config.Sites;
            this.startIen = config.StartIen;
        }

        public ExtractorConfiguration convertToExtractorConfiguration()
        {
            ExtractorConfiguration config = new ExtractorConfiguration();
            config.CRON = this.cron;
            config.ExtractMode = (ExtractorMode)Enum.Parse(typeof(ExtractorMode), this.extractorMode);
            config.MaxRecordsPerQuery = this.maxRecordsPerQuery;
            config.SiteCode = this.siteCode;
            config.Sites = this.sites;
            config.StartIen = this.startIen;
            config.BatchId = this.batchId;

            config.QueryConfigurations = this.queryConfigurations.convertToTree();

            return config;
        }
    }

    #region Lite Messaging

    [Serializable]
    public class ExtractorConfigurationTOLite
    {
        public string siteCode;
        public string vistaFile;
        public string batchId;

        public ExtractorConfigurationTOLite() { }

        public ExtractorConfigurationTOLite(ExtractorConfiguration config)
        {
            this.siteCode = config.SiteCode;
            this.vistaFile = config.QueryConfigurations.RootNode.Value.File;
            this.batchId = config.BatchId;
        }

        public ExtractorConfigurationTOLite(ExtractorConfigurationTO config)
        {
            this.siteCode = config.siteCode;
            this.vistaFile = config.queryConfigurations.rootNode.value.file;
            this.batchId = config.batchId;
        }
    }
    #endregion
}
