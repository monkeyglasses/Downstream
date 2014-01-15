using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using com.bitscopic.downstream.domain.data;
using com.bitscopic.downstream.service;
using com.bitscopic.downstream.utils;
using System.Data;

namespace com.bitscopic.downstream.domain
{
    [Serializable]
    public class ExtractorConfiguration
    {
        /// <summary>
        /// Used to pass distinct list of IENs to extractor for specific extraction cases (e.g. DIFF mode)
        /// </summary>
        public IList<String> SqlIens;

        public Tree<QueryConfiguration> QueryConfigurations;

        /// <summary>
        /// The ID assigned for this configuration - used for tracking groups of extractions
        /// </summary>
        public String BatchId;

        /// <summary>
        /// The mode in which the extractor typically runs
        /// </summary>
        //private ExtractorMode _extractMode;
        public ExtractorMode ExtractMode;
        
        /// <summary>
        /// Maximum number of records to request with every query
        /// </summary>
        public string MaxRecordsPerQuery;

        /// <summary>
        /// The schedule this job should run with
        /// </summary>
        public string CRON;
               
        /// <summary>
        /// The site code at which to run the specified extractor configuration 
        /// </summary>
        public string SiteCode;
               
        /// <summary>
        /// Can be used to narrow extraction sites (e.g. 1) 506,515 2) V11 3) R3)
        /// </summary>
        public string Sites;
        
        /// <summary>
        /// Used to provide an extractor with the start point for a query
        /// </summary>
        public string StartIen;

        /// <summary>
        /// For capability to stop at specified IEN
        /// </summary>
        public String EndIen;

        /// <summary>
        /// The number of connections this configuration will create to a single vista
        /// used to perform multiple value processing
        /// </summary>
        public UInt16 MaxConnectionPoolSize;

        /// <summary>
        /// A list of all the orchestrators in the database
        /// </summary>
        public IList<string> AllOrchestrators;

        /// <summary>
        /// The semi-colon delimited list of sql commands to execute before placing this job on the work stack
        /// </summary>
        public string ON_START;

        /// <summary>
        /// The semi-colon delimited list of sql commands to execute when the job has completed for all applicable sites
        /// </summary>
        public string ON_COMPLETE;

        /// <summary>
        /// Parameterless constrcutor
        /// </summary>
        public ExtractorConfiguration() { }
        
        /// <summary>
        /// Checks required properties have been set
        /// </summary>
        /// <returns>True if configured correctly, false otherwise</returns>
        public bool isCompleteConfiguration()
        {
            return 
                !String.IsNullOrEmpty(SiteCode) | 
                !String.IsNullOrEmpty(MaxRecordsPerQuery) |
                !String.IsNullOrEmpty(MaxConnectionPoolSize.ToString());
        }

        /// <summary>
        /// Builds a string based on interal propery values
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();

            if (this.QueryConfigurations == null || this.QueryConfigurations.RootNode == null || this.QueryConfigurations.RootNode.Value == null)
            {
                sb.AppendLine("This config doesn't appear to have any query configurations!");
            }
            else
            {
                sb.AppendLine("Config top level file: " + this.QueryConfigurations.RootNode.Value.File);
                sb.AppendLine("Config site: " + this.SiteCode);
                sb.AppendLine("Config start IEN: " + this.StartIen);
            }
            sb.AppendLine("Running in mode: " + Enum.GetName(typeof(ExtractorMode), this.ExtractMode));
            return sb.ToString();
        }

        public ExtractorConfiguration Clone()
        {
            ExtractorConfiguration newConfig = new ExtractorConfiguration()
            {
                BatchId = this.BatchId,
                ExtractMode = this.ExtractMode,
                MaxRecordsPerQuery = this.MaxRecordsPerQuery,
                SiteCode = this.SiteCode,
                StartIen = this.StartIen,
                MaxConnectionPoolSize = this.MaxConnectionPoolSize,
                CRON = this.CRON,
                ON_COMPLETE = this.ON_COMPLETE,
                ON_START = this.ON_START
            };
            newConfig.AllOrchestrators = new List<string>();
            if (this.AllOrchestrators != null && this.AllOrchestrators.Count > 0)
            {
                foreach (string s in this.AllOrchestrators)
                {
                    newConfig.AllOrchestrators.Add(s);
                }
            }
            newConfig.QueryConfigurations = new Tree<QueryConfiguration>(this.QueryConfigurations.RootNode);
            return newConfig;
        }
    }

    [Serializable]
    public class QueryConfiguration
    {
        //bool _packed = true;
        public bool Packed = true;
        public string File;
        public string FullyQualifiedFile;
        public string Fields;
        public string XREF;
        public string Identifier;
        public string IdentifiedFiles;
        public string From;
        public string Part;
        public string Screen;
        public string WP_OR_COMPUTED_FIELDS;
        public string Gets_Alignment;
        public bool HasChildren;
        public DataTable DataTable;
        public DataTable GetsDataTable;

        public QueryConfiguration() { }


        public override bool Equals(object obj)
        {
            if (obj == null || !(obj is QueryConfiguration))
            {
                return false;
            }
            return String.Equals(((QueryConfiguration)obj).File, this.File);
        }

        public void applyDateOverlaps(Int32 pastDays, Int32 futureDays)
        {
            // check FROM param for date placeholders
            if (!String.IsNullOrEmpty(this.From) && this.From.Contains("<FROM_DATE>"))
            {
                this.From = this.From.Replace("<FROM_DATE>", StringUtils.getPastVistaDate(pastDays));
            }
            if (!String.IsNullOrEmpty(this.From) && this.From.Contains("<TO_DATE>"))
            {
                this.From = this.From.Replace("<TO_DATE>", StringUtils.getFutureVistaDate(futureDays));
            }
            if (!String.IsNullOrEmpty(this.From) && this.From.Contains("<DATE>"))
            {
                this.From = this.From.Replace("<DATE>", StringUtils.getVistaDate());
            }

            // check IDENTIFIER param for date placeholders
            if (!String.IsNullOrEmpty(this.Identifier) && this.Identifier.Contains("<FROM_DATE>"))
            {
                this.Identifier = this.Identifier.Replace("<FROM_DATE>", StringUtils.getPastVistaDate(pastDays));
            }
            if (!String.IsNullOrEmpty(this.Identifier) && this.Identifier.Contains("<TO_DATE>"))
            {
                this.Identifier = this.Identifier.Replace("<TO_DATE>", StringUtils.getFutureVistaDate(futureDays));
            }
            if (!String.IsNullOrEmpty(this.Identifier) && this.Identifier.Contains("<DATE>"))
            {
                this.Identifier = this.Identifier.Replace("<DATE>", StringUtils.getVistaDate());
            }

            // check SCREEN param for date placeholders
            if (!String.IsNullOrEmpty(this.Screen) && this.Screen.Contains("<FROM_DATE>"))
            {
                this.Screen = this.Screen.Replace("<FROM_DATE>", StringUtils.getPastVistaDate(pastDays));
            }
            if (!String.IsNullOrEmpty(this.Screen) && this.Screen.Contains("<TO_DATE>"))
            {
                this.Screen = this.Screen.Replace("<TO_DATE>", StringUtils.getFutureVistaDate(futureDays));
            }
            if (!String.IsNullOrEmpty(this.Screen) && this.Screen.Contains("<DATE>"))
            {
                this.Screen = this.Screen.Replace("<DATE>", StringUtils.getVistaDate());
            }



            // check FROM param for INVERSE date placeholders
            if (!String.IsNullOrEmpty(this.From) && this.From.Contains("<INVERSE_FROM_DATE>"))
            {
                this.From = this.From.Replace("<INVERSE_FROM_DATE>", StringUtils.getPastInverseVistaDate(pastDays));
            }
            if (!String.IsNullOrEmpty(this.From) && this.From.Contains("<INVERSE_TO_DATE>"))
            {
                this.From = this.From.Replace("<INVERSE_TO_DATE>", StringUtils.getFutureInverseVistaDate(futureDays));
            }
            if (!String.IsNullOrEmpty(this.From) && this.From.Contains("<INVERSE_DATE>"))
            {
                this.From = this.From.Replace("<INVERSE_DATE>", StringUtils.getInverseVistaDate());
            }

            // check IDENTIFIER param for INVERSE date placeholders
            if (!String.IsNullOrEmpty(this.Identifier) && this.Identifier.Contains("<INVERSE_FROM_DATE>"))
            {
                this.Identifier = this.Identifier.Replace("<INVERSE_FROM_DATE>", StringUtils.getPastInverseVistaDate(pastDays));
            }
            if (!String.IsNullOrEmpty(this.Identifier) && this.Identifier.Contains("<INVERSE_TO_DATE>"))
            {
                this.Identifier = this.Identifier.Replace("<INVERSE_TO_DATE>", StringUtils.getFutureInverseVistaDate(futureDays));
            }
            if (!String.IsNullOrEmpty(this.Identifier) && this.Identifier.Contains("<INVERSE_DATE>"))
            {
                this.Identifier = this.Identifier.Replace("<INVERSE_DATE>", StringUtils.getInverseVistaDate());
            }

            // check SCREEN param for INVERSE date placeholders
            if (!String.IsNullOrEmpty(this.Screen) && this.Screen.Contains("<INVERSE_FROM_DATE>"))
            {
                this.Screen = this.Screen.Replace("<INVERSE_FROM_DATE>", StringUtils.getPastInverseVistaDate(pastDays));
            }
            if (!String.IsNullOrEmpty(this.Screen) && this.Screen.Contains("<INVERSE_TO_DATE>"))
            {
                this.Screen = this.Screen.Replace("<INVERSE_TO_DATE>", StringUtils.getFutureInverseVistaDate(futureDays));
            }
            if (!String.IsNullOrEmpty(this.Screen) && this.Screen.Contains("<INVERSE_DATE>"))
            {
                this.Screen = this.Screen.Replace("<INVERSE_DATE>", StringUtils.getInverseVistaDate());
            }

        }
    }
}
