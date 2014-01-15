using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using gov.va.medora.mdo.dao;

namespace com.bitscopic.downstream.domain
{
    [Serializable]
    public class VistaQuery
    {
        public Int32 QueryErrorCount { get; set; }
        // Set above the query configruation
        string _flags = "IP"; // default value
        public bool IsSubFileQuery { get; set; }
        public String IENS { get; set; }
        public string Flags { get { return _flags; } set { _flags = value; } }
        public string MaxRecords { get; set; }
        public string SiteCode { get; set; }
        public string StartIen { get; set; }
        //public AbstractConnection Connection { get; set; }
        public string Global { get; set; }

        // Set within the query configuration
        public string VistaFile { get; set; }
        public string Fields { get; set; }
        public string XREF { get; set; }
        public string From { get; set; }
        public string Part { get; set; }
        public string Screen { get; set; }
        public string Identifier { get; set; }
        public string IdentifiedFiles { get; set; }
        public string WP_Or_Computed_Fields { get; set; }
        public string Gets_Alignment { get; set; }

        /// <summary>
        /// VistaQuery constructor for use with getLastIen(VistaQuery) function 
        /// </summary>
        /// <param name="sitecode"></param>
        /// <param name="vistaFile"></param>
        public VistaQuery(string sitecode, string vistaFile)
        {
            SiteCode = sitecode;
            VistaFile = vistaFile;
        }

        /// <summary>
        /// VistaQuery constructor for use with query(VistaQuery) function
        /// </summary>
        /// <param name="sitecode"></param>
        /// <param name="vistaFile"></param>
        /// <param name="startIen"></param>
        /// <param name="maxRecords"></param>
        /// <param name="fields"></param>
        public VistaQuery(string sitecode, string vistaFile, string startIen, string maxRecords, string fields)
        {
            SiteCode = sitecode;
            VistaFile = vistaFile;
            StartIen = startIen;
            MaxRecords = maxRecords;
            Fields = fields;
        }

        /// <summary>
        /// VistaQuery constructor for all uses
        /// </summary>
        /// <param name="cxn"></param>
        /// <param name="sitecode"></param>
        /// <param name="vistaFile"></param>
        /// <param name="startIen"></param>
        /// <param name="maxRecords"></param>
        /// <param name="fields"></param>
        //public VistaQuery(/*AbstractConnection cxn, */ string sitecode, string vistaFile, string startIen, string maxRecords, string fields)
        //{
        //    //Connection = cxn;
        //    SiteCode = sitecode;
        //    VistaFile = vistaFile;
        //    StartIen = startIen;
        //    MaxRecords = maxRecords;
        //    Fields = fields;
        //}

        /// <summary>
        /// VistaQuery constructor for use based on a job configuration
        /// </summary>
        /// <param name="config"></param>
        public VistaQuery(ExtractorConfiguration job, QueryConfiguration query)
        {
            SiteCode = job.SiteCode;
            MaxRecords = job.MaxRecordsPerQuery;
            VistaFile = query.File;
            Fields = query.Fields;

            StartIen = (job.StartIen == null || job.StartIen.Equals(String.Empty)) ? "0" : job.StartIen;
            XREF = (query.XREF == null || query.XREF.Equals(String.Empty)) ? "#" : query.XREF;
            From = (query.From == null || query.From.Equals(String.Empty)) ? StartIen : query.From;
            Part = (query.Part == null || query.Part.Equals(String.Empty)) ? String.Empty : query.Part;
            Screen = (query.Screen == null || query.Screen.Equals(String.Empty)) ? String.Empty : query.Screen;
            Identifier = (query.Identifier == null || query.Identifier.Equals(String.Empty)) ? String.Empty : query.Identifier;
            IdentifiedFiles = (query.IdentifiedFiles == null || query.IdentifiedFiles.Equals(String.Empty)) ? String.Empty : query.IdentifiedFiles;
            WP_Or_Computed_Fields = (query.WP_OR_COMPUTED_FIELDS == null || query.WP_OR_COMPUTED_FIELDS.Equals(String.Empty)) ? String.Empty : query.WP_OR_COMPUTED_FIELDS;
            Gets_Alignment = (query.Gets_Alignment == null || query.Gets_Alignment.Equals(String.Empty)) ? String.Empty : query.Gets_Alignment;

            if (!query.Packed)
            {
                _flags = "I";
            }
            // this helper code is mostly for testing purposes - if no site code was specified on the job, try and set the site to the first in the sites list
            //if (String.IsNullOrEmpty(SiteCode) && !String.IsNullOrEmpty(job.Sites))
            //{
            //    SiteCode = job.Sites.Split(new char[] { ';' })[0];
            //}
        }

        public VistaQuery()
        {
            // TODO: Complete member initialization
        }

        /// <summary>
        /// Creates a string of object parameters
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            // FIXME: this needs updating
            return "Site Code: " + SiteCode + Environment.NewLine +
                "File: " + VistaFile + Environment.NewLine +
                "Fields: " + Fields + Environment.NewLine +
                "IENS: " + this.IENS + Environment.NewLine +
                "Start IEN: " + StartIen + Environment.NewLine +
                "Max Records: " + MaxRecords + Environment.NewLine +
                "XREF: " + this.XREF + Environment.NewLine +
                "Screen: " + this.Screen + Environment.NewLine + 
                "Identifier: " + this.Identifier;
        }

        public override bool Equals(object obj)
        {
            if (!(obj is VistaQuery))
            {
                return false;
            }
            VistaQuery qry = (VistaQuery)obj;
            if (String.Equals(this.SiteCode, qry.SiteCode, StringComparison.CurrentCultureIgnoreCase) 
                && String.Equals(this.VistaFile, qry.VistaFile, StringComparison.CurrentCultureIgnoreCase))
            {
                return true;
            }
            return false;
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
    }
}
