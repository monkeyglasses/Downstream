using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using com.bitscopic.downstream.domain;
using com.bitscopic.downstream.domain.data;
using com.bitscopic.downstream.utils;
using gov.va.medora.mdo.api;
using gov.va.medora.mdo;
using gov.va.medora.mdo.dao;
using gov.va.medora.mdo.dao.vista;
using System.Data;
using System.Threading;
//using System.Threading.Tasks;
using System.Collections.Concurrent;
using com.bitscopic.downstream.domain.reporting;
using System.Configuration;
using VistaQuery = com.bitscopic.downstream.domain.VistaQuery;

namespace com.bitscopic.downstream.dao.vista
{
    public class VistaDao
    {
        IVistaDao _svc;

        SiteTable _extractorSites;
        User _downstreamAccount;
        AbstractCredentials _downstreamCreds;
        //AbstractConnection _cxn;
        IList<string> _sitecodes;
        //ConcurrentQueue<AbstractConnection> _connectionPool;
        //ExtractorReport _report; // don't like the idea of a DAO having a report... chucking it
        public ExtractorConfiguration Configuration { get; set; }

        private UInt16 r_depth = 0;
        private string[] emptyStringArray = new string[0];

        public VistaDao()
        {
            //_svc = new MdoVistaDao();
            // defaulting to this for now but should make it configurable to use either WCF or MDO directly
           // _svc = new WcfVistaDao(new Uri(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.QuerySvcURL]));

            _extractorSites = new SiteTable(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.VhaSitesFilePath]); // "resources/xml/VhaSites.xml");
            _sitecodes = new List<string>(_extractorSites.Sites.Count);
            foreach (Site site in _extractorSites.Sites.Values)
            {
                _sitecodes.Add(site.Id);
            }
           // _downstreamAccount = getDownstreamUser();
           // _downstreamCreds = getDownstreamCredentials(_downstreamAccount);
        }

        /// <summary>
        /// Get a random site from the cached site table. Waits 10 milliseconds before calculating
        /// each random value to help guard against multiple repeat values
        /// </summary>
        /// <returns>A Site from the sites file</returns>
        public Site getRandomSite()
        {
            System.Threading.Thread.Sleep(10);
            Random r = new Random();
            string randomSitecode = _sitecodes[r.Next(_sitecodes.Count)];
            return _extractorSites.Sites[randomSitecode] as Site;
        }

        /// <summary>
        /// Recursively populate all of our required data tables
        /// </summary>
        /// <param name="queryConfigurationNode"></param>
        internal void initializeDataTable(TreeNode<QueryConfiguration> queryConfigurationNode)
        {
            QueryConfiguration config = queryConfigurationNode.Value;
            String wp_or_computed = config.WP_OR_COMPUTED_FIELDS;
            String gets_alignment = config.Gets_Alignment;
            bool hasVerticalAlignment = (gets_alignment == null || gets_alignment.Equals(String.Empty)) ? false : true;
            string [] wp_or_computed_array = new string[0];
            if (wp_or_computed == null || wp_or_computed.Equals(String.Empty)) { }
            else
            {
                if (!hasVerticalAlignment)
                {
                    wp_or_computed_array = wp_or_computed.Split(new char[] { ';' });
                }
            }
            if (queryConfigurationNode.Depth == 0)
            {
                config.DataTable = DataTableUtils.generateVistaQueryDataTable(config.File, config.Fields.Split(new char[] { ';' }), false, wp_or_computed_array);
            }
            else
            {
                config.DataTable = DataTableUtils.generateVistaQueryDataTable(config.File, config.Fields.Split(new char[] { ';' }), true, wp_or_computed_array);
            }
            if (hasVerticalAlignment)
            {
                if (queryConfigurationNode.Depth == 0)
                {
                    config.GetsDataTable = DataTableUtils.generateVerticalDataTable(config.File + "_KEYVALUE", false);
                }
                else
                {
                    config.GetsDataTable = DataTableUtils.generateVerticalDataTable(config.File + "_KEYVALUE", true);
                }
            }
            foreach (TreeNode<QueryConfiguration> childNode in queryConfigurationNode.Children)
            {
                initializeDataTable(childNode);
            }
            ++r_depth;
            if (hasVerticalAlignment)
            {
                ++r_depth;
            }
        }

        /// <summary>
        /// Build queues to store IENS values which are resolved via an identifier
        /// </summary>
        /// <param name="files">The files the identifier resolves</param>
        /// <returns></returns>
        private ConcurrentQueue<string>[] buildIENQueues(String files)
        {
            int identifiedCount = (files == null || files.Equals(String.Empty)) ? 0 : files.Split(';').Length;
            ConcurrentQueue<string>[] retVal = new ConcurrentQueue<string>[identifiedCount];
            for (int i = 0; i < identifiedCount; ++i)
            {
                retVal[i] = new ConcurrentQueue<string>();
            }
            return retVal;
        }


        private void packageGetsQueryResultsVertically
        (
            string p_ien,
            string ien, 
            int intSiteCode, 
            DateTime retrievalTime,
            Dictionary<string,string> computed_or_word_processing_values,
            DataTable verticalTable
        )
        {
            foreach (string key in computed_or_word_processing_values.Keys)
            {
                // allFields will hold the decimal IEN, sitecode, timestamp 
                // in addition to the returned values from the DDR query
                // and the values from the GETS query
                int pColSwitch = (p_ien.Equals("-1")) ? 0 : 1;

                // 3 or 4 table headers, the p_ien if needed and two for key/value
                object[] allFields = new object[3 + pColSwitch + 2];

                // keep track of our current location
                int curIdx = 3;

                // Set up our headers based on whether or not this is a child file
                if (pColSwitch == 0)
                {
                    allFields[0] = ien;
                    allFields[1] = intSiteCode;
                    allFields[2] = retrievalTime;
                }
                else
                {
                    allFields[0] = p_ien;
                    allFields[1] = ien;
                    allFields[2] = intSiteCode;
                    allFields[3] = retrievalTime;
                    ++curIdx;
                }

                allFields[curIdx++] = key;
                allFields[curIdx++] = computed_or_word_processing_values[key];
                verticalTable.Rows.Add(allFields);
            }
        }
            

        /// <summary>
        /// Package a result row into an appopriately formed object[] for insert into a specific
        /// format datatable
        /// </summary>
        /// <param name="resolvedFilesCount">The number of fields resolved in the list dic query via the identifier</param>
        /// <param name="ddrValues">The results of the list dic query</param>
        /// <param name="p_ien">The ien for the parent or -1 if this is a parent file</param>
        /// <param name="ien">The ien for this row</param>
        /// <param name="intSiteCode">The site code</param>
        /// <param name="retrievalTime">The retrieval time</param>
        /// <param name="multipleIens">The list of iens that are filtered if using an identifier</param>
        /// <param name="computed_or_word_processing_values">THe results of the ddr gets query</param>
        /// <returns></returns>
        private object[] packageVistaQueryResults
        ( 
            int expectedColumnCount,
            int resolvedFilesCount,
            string [] ddrValues, 
            string p_ien,
            string ien, 
            int intSiteCode, 
            DateTime retrievalTime,
            ConcurrentQueue<string>[] multipleIens,
            string [] computed_or_word_processing_values
        )
        {
            // allFields will hold the decimal IEN, sitecode, timestamp 
            // in addition to the returned values from the DDR query
            // and the values from the GETS query
            int pColSwitch = (p_ien.Equals("-1")) ? 0 : 1;

            // 3 or 4 table headers, the ddr fields, -1 for the IEN, -X for multiples + X for word processing and computed fields
            object[] allFields = new object[3 + pColSwitch + ddrValues.Length - 1 - resolvedFilesCount + computed_or_word_processing_values.Length];

            // Set up our headers based on whether or not this is a child file
            if (pColSwitch == 0)
            {
                allFields[0] = ien;
                allFields[1] = intSiteCode;
                allFields[2] = retrievalTime;
            }
            else
            {
                allFields[0] = p_ien;
                allFields[1] = ien;
                allFields[2] = intSiteCode;
                allFields[3] = retrievalTime;
            }

            // For each ddr item, ignoring IEN and anything added by the identifier
            for (int i = 0; i < (ddrValues.Length - 1 - resolvedFilesCount); i++)
            {
                allFields[3 + i + pColSwitch] = ddrValues[i + 1]; // skip the IEN in ddrValues
            }

            // Add any of our word processing or computed fields
            int curIdx = 3 + pColSwitch + ddrValues.Length - 1 - resolvedFilesCount;
            for (int i = 0; i < computed_or_word_processing_values.Length; ++i)
            {
                allFields[curIdx + i] = computed_or_word_processing_values[i];
            }

            // Now populate the iens children for each one resolved by the identifier
            for (int i = 0; i < resolvedFilesCount; ++i)
            {
                int multipleCount = Convert.ToInt32(ddrValues[ddrValues.Length - i - 1]);
                if (multipleCount > 0)
                {
                    if (pColSwitch == 0)
                    {
                        multipleIens[i].Enqueue(ddrValues[0]);
                    }
                    else
                    {
                        multipleIens[i].Enqueue(ddrValues[0] + "," + p_ien.Replace("_", ","));
                    }
                }
            }

            // Protect against results that are too short or too long
            if (allFields.Length != expectedColumnCount)
            {
                int obtainedColumnCount = allFields.Length;
                //_report.addDebug("Expected column count:" + expectedColumnCount + " but received:" + obtainedColumnCount + " column count.");
                object [] tempRetVal = new object[expectedColumnCount];
                for (int i = 0; i < expectedColumnCount; ++i)
                {
                    if (i >= obtainedColumnCount)
                    {
                        tempRetVal[i] = String.Empty;
                    }
                    else
                    {
                        tempRetVal[i] = allFields[i];
                    }
                }
                allFields = tempRetVal;
            }

            // All done!
            return allFields;
        }

        /// <summary>
        /// Process the results of a gets query by packaging up multi-line strings into
        /// an array which holds one string per field queried
        /// </summary>
        /// <param name="getsResults">The raw gets query results</param>
        /// <returns>The properly pacakaged results array</returns>
        private string[] processGetsEntryResults(string[] getsResults, int size)
        {
            // Set up
            string[] retVal = new string[size];
            StringBuilder buffer = new StringBuilder();
            int curIdx = 0;
            bool wpScope = false;

            // For each item in the results data, determine where it should be
            // stored and what parts should be included
            foreach (string result in getsResults)
            {
                // Ignore the data header
                if (result.Equals("[Data]"))
                {
                    continue;
                }

                // Ignore empty headers and footers
                else if (result.Equals(String.Empty))
                {
                    continue;
                }

                // We've reached the end of a word-processing field
                else if (result.Equals("$$END$$"))
                {
                    retVal[curIdx] = buffer.ToString();
                    buffer = new StringBuilder();
                    wpScope = false;
                    ++curIdx;
                    continue;
                }

                // This is likely the interior of a word-processing field
                else if (wpScope)
                {
                    buffer.Append(result);
                    continue;
                }

                // Process either the header to a word-processing field, or the data for a computed field
                else if (result.Contains("^"))
                {
                    string[] parts = result.Split(new char[] { '^' });
                    if (result.EndsWith("[WORD PROCESSING]"))
                    {
                        wpScope = true;
                    }
                    else
                    {
                        string computedWithSeperator = result.Substring(result.IndexOf(parts[2]) + parts[2].Length + 1);
                        retVal[curIdx] = computedWithSeperator.Remove(computedWithSeperator.Length - 1);
                        ++curIdx;
                    }
                }

                // Shouldn't ever be here
                else
                {
                    continue;
                }
            }

            // Fill in anything that we couldn't find in the raw data
            for (int i = 0; i < retVal.Length; ++i)
            {
                if (retVal[i] == null)
                {
                    retVal[i] = String.Empty;
                }
            }

            // All done!
            return retVal;
        }

        /// <summary>
        /// Process the results of a gets query by packaging up multi-line strings into
        /// an array which holds the gets results vertically aligned
        /// </summary>
        /// <param name="getsResults">The raw gets query results</param>
        /// <returns>The properly pacakaged results array</returns>
        private Dictionary<string,string> processGetsEntryResults(string[] getsResults)
        {
            // Set up
            Dictionary<string, string> retVal = new Dictionary<string, string>();

                // For each item in the results data, determine where it should be
                // stored and what parts should be included
            foreach (string result in getsResults)
            {
                // Ignore the data header
                if (result.Equals("[Data]"))
                {
                    continue;
                }

                // Ignore empty headers and footers
                else if (result.Equals(String.Empty))
                {
                    continue;
                }

                // Process either the header to a word-processing field, or the data for a computed field
                else if (result.Contains("^"))
                {
                    string[] parts = result.Split(new char[] { '^' });
                    retVal.Add(parts[2], parts[3]);
                }

                // Shouldn't ever be here
                else
                {
                    continue;
                }
            }

            // All done!
            return retVal;
        }

        /// <summary>
        /// Execute a DDR Lister query based on the VistaQuery properties
        /// </summary>
        /// <param name="query">
        /// A VistaQuery object specifying the site, Vista file and fields, start point
        /// and the maximum number of records for retrieval
        /// </param>
        /// <returns>
        /// Returns a System.Data.DataTable specially formatted to match the decided upon Downstream database
        /// table structure. No data will be transformed but extra data will be added to facilitate certain actions.
        /// 
        /// The first column contains the record IEN
        /// The second column contains the sitecode from which the record was retrieved (e.g. 506)
        /// The third column contains the DDR Lister completion time (record retrieval time)
        /// The other columns are those specified by the Query.Fields property
        /// </returns>
        public DataTable[] query(VistaQuery query)
        {
            // Protect
            if (query == null || String.IsNullOrEmpty(query.Fields) || String.IsNullOrEmpty(query.VistaFile) || 
                String.IsNullOrEmpty(query.StartIen) || String.IsNullOrEmpty(query.MaxRecords))
            {
                throw new ApplicationException("The query has not been setup properly");
            }
            
            // Get a handle on our root node
            TreeNode<QueryConfiguration> rootNode = Configuration.QueryConfigurations.RootNode;

            // Set up our various data tables
            r_depth = 0;
            initializeDataTable(rootNode);
            DataTable results = rootNode.Value.DataTable;
            DataTable vertResults = rootNode.Value.GetsDataTable;
            DataTable[] retVal = new DataTable[r_depth];

            // Set up our GETS word processing or computed fields variables
            bool hasWPOrComputedFields = false;
            bool hasGetsVertical = false;
            int wpOrComputedFieldsCount = 0;
            if( query.WP_Or_Computed_Fields == null || query.WP_Or_Computed_Fields.Equals(String.Empty) )
            {
            }
            else
            {
                hasWPOrComputedFields = true;
                wpOrComputedFieldsCount = query.WP_Or_Computed_Fields.Split( new char[] {';'} ).Length;
                hasGetsVertical = (query.Gets_Alignment == null || query.Gets_Alignment.Equals(String.Empty)) ? false : true;
            }

            // Set up our collections to store the IENS which match the identifier being used
            ConcurrentQueue<string>[] queues = buildIENQueues(query.IdentifiedFiles);

            // All set
            //AbstractConnection topLevelConn = null;
            try
            {
                // Set up our connection
                //if (!_connectionPool.TryDequeue(out topLevelConn))
                //{
                //    throw new ApplicationException("The connection pool has no available connections for the top level query!");
                //}

                // Prepare to make our top-level query
                //ToolsApi api = new ToolsApi();
                //WcfVistaDao _svc = new WcfVistaDao(new Uri(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.QuerySvcURL]));
                string[] ddrResults = null;

                #region gets tests
                // WORKS
                // VistaToolsDao vtd = new VistaToolsDao(topLevelConn);
                // String test = vtd.getFieldAttribute("63", ".01", "LABEL");                               // get a file/field attribute
                // Object test = vtd.getVariableValue("$G(^OR(100,0))");                                    // get a file header with info on ien and count
                // ddrResults = api.ddrGetsEntry(topLevelConn, "60.01", "72,6440,", "5.5", "I");               // get a word-processing or computed field
                //ddrResults = api.ddrGetsEntry(topLevelConn, "772", "769858542,", "200", "I");
                //string [] getrest = processGetsEntryResults(ddrResults, 1);
                //tearDownConnection(topLevelConn);

                //string[] getsResults2 = api.ddrGetsEntry(topLevelConn, "63.3", "7069074.7943,79,", ".01", "N");
                //getsResults2 = api.ddrGetsEntry(topLevelConn, "63.3", "7069074.7943,79,", ".01", "I");
                //getsResults2 = api.ddrGetsEntry(topLevelConn, "63.3", "7069074.7943,79,", ".01", "P");
                //getsResults2 = api.ddrGetsEntry(topLevelConn, "63.3", "7069074.7943,79,", ".01", "R");
                //getsResults2 = api.ddrGetsEntry(topLevelConn, "63.3", "79,7069074.7943,", ".01", "N");
                //getsResults2 = api.ddrGetsEntry(topLevelConn, "63.3", "79,7069074.7943,", ".01", "I");
                //getsResults2 = api.ddrGetsEntry(topLevelConn, "63.3", "79,7069074.7943,", ".01", "P");
                //getsResults2 = api.ddrGetsEntry(topLevelConn, "63.3", "79,7069074.7943,", ".01", "R");

                //string[] getsResults2 = api.ddrGetsEntry(topLevelConn, "63.3", "1,7078781.8655,54,", ".01", "I");
                //string[] getsResults3 = api.ddrGetsEntry(topLevelConn, "63.3", "1,7078781.8655,54,", "0:1", "I");
                //string[] getsResults4 = api.ddrGetsEntry(topLevelConn, "63.3", "1,7078781.8655,54,", "*", "I");
                //string[] getsResults5 = api.ddrGetsEntry(topLevelConn, "63.3", "1,7078781.8655,54,", "**", "I");

                //string[] getsResults31 = api.ddrGetsEntry(topLevelConn, "63.3", "1,6919479.826978,85,", "2:3", "IN");
                //string[] getsResults41 = api.ddrGetsEntry(topLevelConn, "63.3", "1,7078781.8655,54,", "*", "IN");
                //string[] getsResults51 = api.ddrGetsEntry(topLevelConn, "63.3", "1,7078781.8655,54,", "**", "IN");

                //string[] getsResults32 = api.ddrGetsEntry(topLevelConn, "63.3", "1,6919479.84515,5276,", "2:3", "NI");
                //string[] getsResults42 = api.ddrGetsEntry(topLevelConn, "63.3", "1,7078781.8655,54,", "*", "NI");
                //string[] getsResults52 = api.ddrGetsEntry(topLevelConn, "63.3", "1,7078781.8655,54,", "**", "NI");

                //string[] getsResults6 = api.ddrGetsEntry(topLevelConn, "63.05", "7078781.8655,54,", ".01", "I");
                //string[] getsResults7 = api.ddrGetsEntry(topLevelConn, "63.05", "7078781.8655,54,", "0:1", "I");
                //string[] getsResults8 = api.ddrGetsEntry(topLevelConn, "63.05", "7078781.8655,54,", "*", "I");
                //string[] getsResults9 = api.ddrGetsEntry(topLevelConn, "63.05", "7078781.8655,54,", "**", "I");
                //string[] getsResults10 = api.ddrGetsEntry(topLevelConn, "63.05", "7078781.8655,54,", "**", String.Empty);
                //string[] getsResults11 = api.ddrGetsEntry(topLevelConn, "63.05", "7078781.8655,54,", "**", "IN");
                //string[] getsResults12 = api.ddrGetsEntry(topLevelConn, "63.05", "7078781.8655,54,", "**", "NI");

                //string[] getsResults31 = api.ddrGetsEntry(topLevelConn, "63.3", "1,7019493.876,61,", "2.0000:2.9999", "IN");
                //packageGetsQueryResultsVertically(
                //    "10",
                //    "1",
                //    640,
                //    DateTime.Now,
                //    processGetsEntryResults(getsResults31),
                //    rootNode.Children[0].Children[1].Value.GetsDataTable
                //    );
                //topLevelConn.disconnect();

                //string[] ddrResults40 = api.ddrLister(
                //    topLevelConn,
                //    "69.03",
                //    ",382,3110120,",
                //    ".01;3",
                //    "IP",
                //    "10",
                //    "",
                //    "",
                //    "#",
                //    "",
                //    ""
                //);
                //string[] ddrResults41 = api.ddrLister(
                //    topLevelConn,
                //    "69.03",
                //    ",382,3110120,",
                //    ".01;3",
                //    "IP",
                //    "",
                //    "",
                //    "",
                //    "#",
                //    "",
                //    ""
                //);
                //string[] ddrResults42 = api.ddrLister(
                //    topLevelConn,
                //    "69.03",
                //    ",382,3110120,",
                //    ".01;3",
                //    "IP",
                //    "10",
                //    "",
                //    "",
                //    "#",
                //    "I $P($G(^LRO(68,$P($G(^(0)),U,4),0)),U,2)=\\\"MI\\\"",
                //    ""
                //);
                //topLevelConn.disconnect();
                #endregion

                #region IEN seeding for files with date indexes here
                if (query.VistaFile.Equals("52") && (query.From == null || query.From.Equals(String.Empty) || query.From.Equals("0")))
                {
                    ddrResults = _svc.ddrLister(query.SiteCode, query.VistaFile, "", query.Fields, query.Flags, "1", "3091231", "", "AC", "", ""); 
                    //ddrResults = api.ddrLister
                    //(
                    //    topLevelConn,
                    //    query.VistaFile,
                    //    "",
                    //    query.Fields,
                    //    query.Flags,
                    //    "1",
                    //    "3091231",
                    //    "",
                    //    "AC",
                    //    "",
                    //    ""
                    //);
                    if (ddrResults == null || ddrResults.Length == 0)
                    {
                        //_connectionPool.Enqueue(topLevelConn);
                        retVal[0] = results;
                        return retVal;
                    }
                    query.From = ddrResults[0].Split('^')[0];
                }
                if (query.VistaFile.Equals("100") && (query.From == null || query.From.Equals(String.Empty) || query.From.Equals("0")))
                {
                    ddrResults = _svc.ddrLister(query.SiteCode, query.VistaFile, "", query.Fields, query.Flags, "1", "3100101", "", "AF", "", "");
                    //ddrResults = api.ddrLister
                    //(
                    //    topLevelConn,
                    //    query.VistaFile,
                    //    "",
                    //    query.Fields,
                    //    query.Flags,
                    //    "1",
                    //    "3100101",
                    //    "",
                    //    "AF",
                    //    "",
                    //    ""
                    //);
                    if (ddrResults == null || ddrResults.Length == 0)
                    {
                        //_connectionPool.Enqueue(topLevelConn);
                        retVal[0] = results;
                        return retVal;
                    }
                    query.From = ddrResults[0].Split('^')[0];
                }
                if (query.VistaFile.Equals("120.5") && (query.From == null || query.From.Equals(String.Empty) || query.From.Equals("0")))
                {
                    ddrResults = _svc.ddrLister(query.SiteCode, query.VistaFile, "", query.Fields, query.Flags, "1", "3100101", "", "B", "", "");
                    //ddrResults = api.ddrLister
                    //(
                    //    topLevelConn,
                    //    query.VistaFile,
                    //    "",
                    //    query.Fields,
                    //    query.Flags,
                    //    "1",
                    //    "3100101",
                    //    "",
                    //    "B",
                    //    "",
                    //    ""
                    //);
                    if (ddrResults == null || ddrResults.Length == 0)
                    {
                        //_connectionPool.Enqueue(topLevelConn);
                        retVal[0] = results;
                        return retVal;
                    }
                    query.From = ddrResults[0].Split('^')[0];
                }
                if (query.VistaFile.Equals("9000010") && (query.From == null || query.From.Equals(String.Empty) || query.From.Equals("0")))
                {
                    ddrResults = _svc.ddrLister(query.SiteCode, query.VistaFile, "", query.Fields, query.Flags, "1", "3100101", "", "B", "", "");
                    //ddrResults = api.ddrLister
                    //(
                    //    topLevelConn,
                    //    query.VistaFile,
                    //    "",
                    //    query.Fields,
                    //    query.Flags,
                    //    "1",
                    //    "3100101",
                    //    "",
                    //    "B",
                    //    "",
                    //    ""
                    //);
                    if (ddrResults == null || ddrResults.Length == 0)
                    {
                        //_connectionPool.Enqueue(topLevelConn);
                        retVal[0] = results;
                        return retVal;
                    }
                    query.From = ddrResults[0].Split('^')[0];
                }
                if (query.VistaFile.Equals("356") && (query.From == null || query.From.Equals(String.Empty) || query.From.Equals("0")))
                {
                    ddrResults = _svc.ddrLister(query.SiteCode, query.VistaFile, "", query.Fields, query.Flags, "1", "3100101", "", "D", "", "");
                    //ddrResults = api.ddrLister
                    //(
                    //    topLevelConn,
                    //    query.VistaFile,
                    //    "",
                    //    query.Fields,
                    //    query.Flags,
                    //    "1",
                    //    "3100101",
                    //    "",
                    //    "D",
                    //    "",
                    //    ""
                    //);
                    if (ddrResults == null || ddrResults.Length == 0)
                    {
                        //_connectionPool.Enqueue(topLevelConn);
                        retVal[0] = results;
                        return retVal;
                    }
                    query.From = ddrResults[0].Split('^')[0];
                }
                if (query.VistaFile.Equals("405") && (query.From == null || query.From.Equals(String.Empty) || query.From.Equals("0")))
                {
                    ddrResults = _svc.ddrLister(query.SiteCode, query.VistaFile, "", query.Fields, query.Flags, "1", "3100101", "", "B", "", "");
                    //ddrResults = api.ddrLister
                    //(
                    //    topLevelConn,
                    //    query.VistaFile,
                    //    "",
                    //    query.Fields,
                    //    query.Flags,
                    //    "1",
                    //    "3100101",
                    //    "",
                    //    "B",
                    //    "",
                    //    ""
                    //);
                    if (ddrResults == null || ddrResults.Length == 0)
                    {
                        //_connectionPool.Enqueue(topLevelConn);
                        retVal[0] = results;
                        return retVal;
                    }
                    query.From = ddrResults[0].Split('^')[0];
                }
                if (query.VistaFile.Equals("45") && (query.From == null || query.From.Equals(String.Empty) || query.From.Equals("0")))
                {
                    ddrResults = _svc.ddrLister(query.SiteCode, query.VistaFile, "", query.Fields, query.Flags, "1", "3100101", "", "AF", "", "");
                    //ddrResults = api.ddrLister
                    //(
                    //    topLevelConn,
                    //    query.VistaFile,
                    //    "",
                    //    query.Fields,
                    //    query.Flags,
                    //    "1",
                    //    "3100101",
                    //    "",
                    //    "AF",
                    //    "",
                    //    ""
                    //);
                    if (ddrResults == null || ddrResults.Length == 0)
                    {
                        //_connectionPool.Enqueue(topLevelConn);
                        retVal[0] = results;
                        return retVal;
                    }
                    query.From = ddrResults[0].Split('^')[0];
                }
                #endregion

                #region IEN seeding for 9000010.07 without date indexes, use an intelligent join into VISIT index, then utilize the date index there
                if (query.VistaFile.Equals("9000010.07") && (query.From == null || query.From.Equals(String.Empty) || query.From.Equals("0")))
                {
                    ddrResults = _svc.ddrLister(query.SiteCode, "9000010", "", ".01", query.Flags, "50", "3100101", "", "B", "", "");
                    //ddrResults = api.ddrLister
                    //(
                    //    topLevelConn,
                    //    "9000010",
                    //    "",
                    //    ".01",
                    //    query.Flags,
                    //    "50",
                    //    "3100101",
                    //    "",
                    //    "B",
                    //    "",
                    //    ""
                    //);
                    if (ddrResults == null || ddrResults.Length == 0)
                    {
                        //_connectionPool.Enqueue(topLevelConn);
                        retVal[0] = results;
                        return retVal;
                    }
                    int lowestVisitIEN = 0;
                    int swapIEN = 0;
                    foreach (string result in ddrResults)
                    {
                        swapIEN = Convert.ToInt32(result.Split('^')[0]);
                        if (swapIEN > lowestVisitIEN)
                        {
                            lowestVisitIEN = swapIEN;
                        }
                    }
                    ddrResults = _svc.ddrLister(query.SiteCode, "9000010.07", "", ".01", query.Flags, "1", (--lowestVisitIEN).ToString(), "", "AD", "", "");
                    //ddrResults = api.ddrLister
                    //(
                    //    topLevelConn,
                    //    "9000010.07",
                    //    "",
                    //    ".01",
                    //    query.Flags,
                    //    "1",
                    //    --lowestVisitIEN + "",
                    //    "",
                    //    "AD",
                    //    "",
                    //    ""
                    //);
                    if (ddrResults == null || ddrResults.Length == 0)
                    {
                        //_connectionPool.Enqueue(topLevelConn);
                        retVal[0] = results;
                        return retVal;
                    }
                    query.From = ddrResults[0].Split('^')[0];
                    //_report.addDebug("Inferred lowest IEN for 9000010.07 sitecode:" + query.SiteCode + " as:" + query.From);
                }
                #endregion
                
                #region IEN seeding for 9000010.18 without date indexes, use a learning halfwise partition search (binary)
                if (query.VistaFile.Equals("9000010.18") && (query.From == null || query.From.Equals(String.Empty) || query.From.Equals("0")))
                {
                    // get the current highest to start
                    ddrResults = _svc.ddrLister(query.SiteCode, query.VistaFile, "", "1201", "IPB", "1", "", "", "#", "", "");
                    //ddrResults = api.ddrLister
                    //(
                    //    topLevelConn,
                    //    query.VistaFile,
                    //    String.Empty,
                    //    "1201",
                    //    "IPB",
                    //    "1",
                    //    String.Empty,
                    //    String.Empty,
                    //    "#",
                    //    String.Empty,
                    //    String.Empty
                    //);
                    if (ddrResults == null || ddrResults.Length == 0)
                    {
                        //_connectionPool.Enqueue(topLevelConn);
                        retVal[0] = results;
                        return retVal;
                    }
                    int lastIEN = Convert.ToInt32(ddrResults[0].Split('^')[0]);
                    int trackHighest = lastIEN;
                    int currentIEN = lastIEN / 2;
                    string matchingExpressionStr1 = "3100101";
                    string matchingExpressionStr2 = "3100102";
                    string matchingExpressionStr3 = "3100103";
                    string matchingExpressionStr4 = "3091231";
                    string matchingExpressionStr5 = "3091230";

                    int matchingExpressionInt = 3100101;
                    bool foundIEN = false;
                    int iterations = 0;

                    while (!foundIEN)
                    {
                        ++iterations;
                        int resultSetMatchDate = -1;
                        if (iterations >= 50)
                        {
                            //_report.addDebug("Unable to infer ien by binary search");   
                            lastIEN = (trackHighest / 4) * 3;
                            query.From = lastIEN + "";
                            //_report.addDebug("Using 3/4 which results in: " + lastIEN);
                            break;
                        }
                        ddrResults = _svc.ddrLister(query.SiteCode, query.VistaFile, "", "1201", "IP", "1000", currentIEN.ToString(), "", "#", "", "");
                        //ddrResults = api.ddrLister
                        //(
                        //    topLevelConn,
                        //    query.VistaFile,
                        //    String.Empty,
                        //    "1201",
                        //    "IP",
                        //    "1000",
                        //    currentIEN + "",
                        //    String.Empty,
                        //    "#",
                        //    String.Empty,
                        //    String.Empty
                        //);
                        if (ddrResults == null || ddrResults.Length == 0)
                        {
                            //_connectionPool.Enqueue(topLevelConn);
                            retVal[0] = results;
                            return retVal;
                        }
                        foreach (string result in ddrResults)
                        {
                            string matchCheck = result.Split('^')[1];
                            if (matchCheck.Equals(String.Empty))
                            {
                                continue;
                            }
                            else
                            {
                                string swap = matchCheck.Split('.')[0];
                                if (swap.Equals(matchingExpressionStr1) ||
                                    swap.Equals(matchingExpressionStr2) ||
                                    swap.Equals(matchingExpressionStr3) ||
                                    swap.Equals(matchingExpressionStr4) ||
                                    swap.Equals(matchingExpressionStr5))
                                {
                                    query.From = result.Split('^')[0];
                                    foundIEN = true;
                                    break;
                                }
                                else
                                {
                                    resultSetMatchDate = Convert.ToInt32(swap);
                                }
                            }
                        }
                        if (foundIEN)
                        {
                            //_report.addDebug(
                                //"Infered startIEN using halfwise partition for file:" + query.VistaFile + 
                                //", Sitecode:" + query.SiteCode + 
                                //" as:" + query.From +
                                //" after:" + iterations + " iterations");
                            break;
                        }
                        else
                        {
                            // difference between last ien and current ien
                            int ienDiff = Math.Abs((lastIEN - currentIEN)) / 2;
                            lastIEN = currentIEN;

                            if (resultSetMatchDate == -1)
                            {
                                //_report.addDebug("Unable to infer ien by binary search");
                                lastIEN = (trackHighest / 4) * 3;
                                query.From = lastIEN + "";
                                //_report.addDebug("Using 3/4 which results in: " + lastIEN);
                                break;
                            }
                            else if (resultSetMatchDate > matchingExpressionInt)
                            {
                                currentIEN = currentIEN - ienDiff;
                            }
                            else
                            {
                                currentIEN = currentIEN + ienDiff;
                            }
                        }
                    }
                }
                #endregion

                #region IEN seeding for 53.79 without date indexes, use a learning halfwise partition search (binary)
                if (query.VistaFile.Equals("53.79") && (query.From == null || query.From.Equals(String.Empty) || query.From.Equals("0")))
                {
                    // get the current highest to start
                    ddrResults = _svc.ddrLister(query.SiteCode, query.VistaFile, "", ".06", "IPB", "1", "", "", "#", "", "");
                    //ddrResults = api.ddrLister
                    //(
                    //    topLevelConn,
                    //    query.VistaFile,
                    //    String.Empty,
                    //    ".06",
                    //    "IPB",
                    //    "1",
                    //    String.Empty,
                    //    String.Empty,
                    //    "#",
                    //    String.Empty,
                    //    String.Empty
                    //);
                    if (ddrResults == null || ddrResults.Length == 0)
                    {
                        //_connectionPool.Enqueue(topLevelConn);
                        retVal[0] = results;
                        return retVal;
                    }
                    int lastIEN = Convert.ToInt32(ddrResults[0].Split('^')[0]);
                    int trackHighest = lastIEN;
                    int currentIEN = lastIEN / 2;
                    string matchingExpressionStr1 = "3100101";
                    string matchingExpressionStr2 = "3100102";
                    string matchingExpressionStr3 = "3100103";
                    string matchingExpressionStr4 = "3091231";
                    string matchingExpressionStr5 = "3091230";

                    int matchingExpressionInt = 3100101;
                    bool foundIEN = false;
                    int iterations = 0;

                    while (!foundIEN)
                    {
                        ++iterations;
                        int resultSetMatchDate = -1;
                        if (iterations >= 50)
                        {
                            //_report.addDebug("Unable to infer ien by binary search");
                            lastIEN = trackHighest / 4 * 3;
                            query.From = lastIEN + "";
                           // _report.addDebug("Using 3/4 which results in: " + lastIEN);
                            break;
                        }
                        ddrResults = _svc.ddrLister(query.SiteCode, query.VistaFile, "", ".06", "IP", "50", currentIEN.ToString(), "", "#", "", "");
                        //ddrResults = api.ddrLister
                        //(
                        //    topLevelConn,
                        //    query.VistaFile,
                        //    String.Empty,
                        //    ".06",
                        //    "IP",
                        //    "50",
                        //    currentIEN + "",
                        //    String.Empty,
                        //    "#",
                        //    String.Empty,
                        //    String.Empty
                        //);
                        if (ddrResults == null || ddrResults.Length == 0)
                        {
                            //_connectionPool.Enqueue(topLevelConn);
                            retVal[0] = results;
                            return retVal;
                        }
                        foreach (string result in ddrResults)
                        {
                            string matchCheck = result.Split('^')[1];
                            if (matchCheck.Equals(String.Empty))
                            {
                                continue;
                            }
                            else
                            {
                                string swap = matchCheck.Split('.')[0];
                                if (swap.Equals(matchingExpressionStr1) ||
                                    swap.Equals(matchingExpressionStr2) ||
                                    swap.Equals(matchingExpressionStr3) ||
                                    swap.Equals(matchingExpressionStr4) ||
                                    swap.Equals(matchingExpressionStr5))
                                {
                                    query.From = result.Split('^')[0];
                                    foundIEN = true;
                                    break;
                                }
                                else
                                {
                                    resultSetMatchDate = Convert.ToInt32(swap);
                                }
                            }
                        }
                        if (foundIEN)
                        {
                            //_report.addDebug(
                            //    "Infered startIEN using halfwise partition for file:" + query.VistaFile +
                            //    ", Sitecode:" + query.SiteCode +
                            //    " as:" + query.From +
                            //    " after:" + iterations + " iterations");
                            break;
                        }
                        else
                        {
                            // difference between last ien and current ien
                            int ienDiff = Math.Abs((lastIEN - currentIEN)) / 2;
                            lastIEN = currentIEN;

                            if (resultSetMatchDate == -1)
                            {
                                //_report.addDebug("Unable to infer ien by binary search");
                                lastIEN = (trackHighest / 4) * 3;
                                query.From = lastIEN + "";
                                //_report.addDebug("Using 3/4 which results in: " + lastIEN);
                                break;
                            }
                            else if (resultSetMatchDate > matchingExpressionInt)
                            {
                                currentIEN = currentIEN - ienDiff;
                            }
                            else
                            {
                                currentIEN = currentIEN + ienDiff;
                            }
                        }
                    }
                }
                #endregion

                // File [2] has so many fields that the results cannot be packaged into a single response. This is a limitation of LIST^DIC
                // that isn't present in GETS^ENTRY, however, putting the long fields in WP_OR_COMPUTED means that a transaction has to occur
                // for each record. This is slow and cumbersome. A better solution is to split the fields you want up and perform seperate
                // DIC calls for each set, then union them back.
                if (query.VistaFile.Equals("2"))
                {
                    #region special [2] handling
                    // Split our fields up into a couple different sets of fields
                    string[] ddrResults1, ddrResults2, ddrResults3, ddrResults4 = null;
                    string queryFields1, queryFields2, queryFields3, queryFields4;
                    queryFields1 = queryFields2 = queryFields3 = queryFields4 = String.Empty;
                    string[] allFields = query.Fields.Split(';');
                    for (int i = 0; i < allFields.Length; ++i)
                    {
                        if (i < 10)
                        {
                            queryFields1 = queryFields1 + allFields[i] + ';';
                        }
                        else if (i < 20)
                        {
                            queryFields2 = queryFields2 + allFields[i] + ';';
                        }
                        else if (i < 30)
                        {
                            queryFields3 = queryFields3 + allFields[i] + ';';
                        }
                        else
                        {
                            queryFields4 = queryFields4 + allFields[i] + ';';
                        }
                    }
                    queryFields1 = queryFields1.Substring(0, queryFields1.Length - 1);
                    queryFields2 = queryFields2.Substring(0, queryFields2.Length - 1);
                    queryFields3 = queryFields3.Substring(0, queryFields3.Length - 1);
                    queryFields4 = queryFields4.Substring(0, queryFields4.Length - 1);

                    // Perform our queries for each set of fields
                    ddrResults1 = _svc.ddrLister(query.SiteCode, query.VistaFile, "", queryFields1, query.Flags, query.MaxRecords, query.From, query.Part, query.XREF, query.Screen, "");
                    //ddrResults1 = api.ddrLister
                    //(
                    //    topLevelConn,
                    //    query.VistaFile,
                    //    "",
                    //    queryFields1,
                    //    query.Flags,
                    //    query.MaxRecords,
                    //    query.From,
                    //    query.Part,
                    //    query.XREF,
                    //    query.Screen,
                    //    String.Empty
                    //);

                    ddrResults2 = _svc.ddrLister(query.SiteCode, query.VistaFile, "", queryFields2, query.Flags, query.MaxRecords, query.From, query.Part, query.XREF, query.Screen, "");
                    //ddrResults2 = api.ddrLister
                    //(
                    //    topLevelConn,
                    //    query.VistaFile,
                    //    "",
                    //    queryFields2,
                    //    query.Flags,
                    //    query.MaxRecords,
                    //    query.From,
                    //    query.Part,
                    //    query.XREF,
                    //    query.Screen,
                    //    String.Empty
                    //);

                    ddrResults3 = _svc.ddrLister(query.SiteCode, query.VistaFile, "", queryFields3, query.Flags, query.MaxRecords, query.From, query.Part, query.XREF, query.Screen, "");
                    //ddrResults3 = api.ddrLister
                    //(
                    //    topLevelConn,
                    //    query.VistaFile,
                    //    "",
                    //    queryFields3,
                    //    query.Flags,
                    //    query.MaxRecords,
                    //    query.From,
                    //    query.Part,
                    //    query.XREF,
                    //    query.Screen,
                    //    String.Empty
                    //);

                    ddrResults4 = _svc.ddrLister(query.SiteCode, query.VistaFile, "", queryFields4, query.Flags, query.MaxRecords, query.From, query.Part, query.XREF, query.Screen, "");
                    //ddrResults4 = api.ddrLister
                    //(
                    //    topLevelConn,
                    //    query.VistaFile,
                    //    "",
                    //    queryFields4,
                    //    query.Flags,
                    //    query.MaxRecords,
                    //    query.From,
                    //    query.Part,
                    //    query.XREF,
                    //    query.Screen,
                    //    query.Identifier
                    //);

                    // protect
                    if (ddrResults1.Length != ddrResults2.Length ||
                        ddrResults2.Length != ddrResults3.Length ||
                        ddrResults3.Length != ddrResults4.Length)
                    {
                        throw new Exception("Incomplete lengths for field split query counts:"
                            + ddrResults1.Length + ","
                            + ddrResults2.Length + ","
                            + ddrResults3.Length + ","
                            + ddrResults4.Length);
                    }
                    ddrResults = new string[ddrResults1.Length];

                    // union our results together
                    for (int i = 0; i < ddrResults1.Length; ++i)
                    {
                        ddrResults[i] =
                            ddrResults1[i] + // capture ien from the first one, ignore it in the rest
                            ddrResults2[i].Substring(ddrResults2[i].IndexOf('^')) +
                            ddrResults3[i].Substring(ddrResults3[i].IndexOf('^')) +
                            ddrResults4[i].Substring(ddrResults4[i].IndexOf('^'));
                    }
                    #endregion
                }
                else
                {
                    // Perform our top-level query
                    ddrResults = _svc.ddrLister(query.SiteCode, query.VistaFile, "", query.Fields, query.Flags, query.MaxRecords, query.From, query.Part, query.XREF, query.Screen, query.Identifier);
                    //ddrResults = api.ddrLister
                    //(
                    //    topLevelConn,
                    //    query.VistaFile,
                    //    "",
                    //    query.Fields,
                    //    query.Flags,
                    //    query.MaxRecords,
                    //    query.From,
                    //    query.Part,
                    //    query.XREF,
                    //    query.Screen,
                    //    query.Identifier
                    //);
                }
                
                // Put this connection back in the pool for re-use later or cleanup if zero results
                //_connectionPool.Enqueue(topLevelConn);

                // Terminate the query once we've hit the end of the file
                if (ddrResults == null || ddrResults.Length == 0)
                {
                    retVal[0] = results;
                    return retVal;
                }

                // Prepare to process our results
                Int16 intSiteCode = Convert.ToInt16(query.SiteCode);
                DateTime retrievalTime = DateTime.Now;

                // Push each valid ddrresult into our datatable
                foreach (string ddrResult in ddrResults)
                {
                    // Break our string up into it's parts
                    string[] ddrValues = ddrResult.Replace("&#94;","^").Split(new char[] { '^' });

                    // Capture and validate the IEN for this entry
                    decimal ien = 0;
                    if (!Decimal.TryParse(ddrValues[0], out ien))
                    {
                        continue;
                    }
                    if (ien == 0)
                    {
                        continue;
                    }
                    // If this query wants a word processing or computed field, go get that now
                    if (hasWPOrComputedFields)
                    {
                        #region WPCOMPUTED
                        try
                        {
                            if (hasGetsVertical)
                            {
                                // Get the value of the computed fields
                                string[] getsResults = _svc.ddrGetsEntry(query.SiteCode, query.VistaFile, String.Concat(ien, ","), query.WP_Or_Computed_Fields, "IN");
                                //string[] getsResults = api.ddrGetsEntry(topLevelConn, query.VistaFile, ien + ",", query.WP_Or_Computed_Fields, "IN");

                                // Package the gets results vertically
                                packageGetsQueryResultsVertically(
                                    "-1",
                                    ien + "",
                                    intSiteCode,
                                    retrievalTime,
                                    processGetsEntryResults(getsResults),
                                    vertResults);

                                // Package everything else normally
                                results.Rows.Add(
                                    packageVistaQueryResults(
                                        results.Columns.Count,
                                        queues.Length,
                                        ddrValues,
                                        "-1",
                                        ien + "",
                                        intSiteCode,
                                        retrievalTime,
                                        queues,
                                        emptyStringArray));
                            }
                            else
                            {
                                // Get the value of the computed fields
                                string[] getsResults = _svc.ddrGetsEntry(query.SiteCode, query.VistaFile, String.Concat(ien, ","), query.WP_Or_Computed_Fields, "I");
                                //string[] getsResults = api.ddrGetsEntry(topLevelConn, query.VistaFile, ien + ",", query.WP_Or_Computed_Fields, "I");

                                // Masasage the data a bit
                                getsResults = processGetsEntryResults(getsResults, wpOrComputedFieldsCount);

                                // Package this record and add it to our table
                                results.Rows.Add(
                                    packageVistaQueryResults(
                                        results.Columns.Count,
                                        queues.Length,
                                        ddrValues,
                                        "-1",
                                        ien + "",
                                        intSiteCode,
                                        retrievalTime,
                                        queues,
                                        getsResults));
                            }
                        }
                        catch (Exception ex)
                        {
                            //_report.addError(ex.Message, ex);
                          //  _report.addError("Error encountered while performing GETS for ien:" + ien + ", file:" + query.VistaFile + ", site:" + query.SiteCode);
                            continue;
                        }
                    }
                    #endregion
                    else
                    {
                        // Package this record and add it to our table
                        results.Rows.Add(
                            packageVistaQueryResults(
                                results.Columns.Count,
                                queues.Length,
                                ddrValues,
                                "-1",
                                ien + "",
                                intSiteCode,
                                retrievalTime,
                                queues,
                                emptyStringArray));
                    }
                }

                // For each of this parent's children, recursively evaluate them against each IEN with data
                // Our queues are populated in backward order, reverse the children and evaluate
                IEnumerable<TreeNode<QueryConfiguration>> children = rootNode.Children.Reverse();
                for (int i = 0; i < children.Count(); ++i)
                {
                    // Only bother recursing for this file if we found IENS that have data for this file
                    if (queues[i].Count > 0)
                    {
                        TreeNode<QueryConfiguration> childNode = children.ElementAt(i);
                        QueryConfiguration childConfig = childNode.Value;
                        VistaQuery childQuery = new VistaQuery(Configuration, childConfig);
                        if (childConfig.From == null || childConfig.From.Equals(String.Empty)) childQuery.From = "0";
                        queryMultiple(childNode, childQuery, queues[i] /*, api */);
                    }
                }
            }
            catch (Exception ex)
            {
                // Errors on the top level query are not recoverable, abort here to the site retry logic in the service above
              //  _report.addError(ex.Message, ex);
                //if (topLevelConn != null && !_connectionPool.Contains(topLevelConn))
                //{
                //    _connectionPool.Enqueue(topLevelConn);
                //}
                throw ex;
            }
    
            // Collapse all of our children data tables into a single returned array
            r_depth = 0;
            collapseDataTables(retVal, rootNode);

            // All done!
            return retVal;
        }

        /// <summary>
        /// Query a multiple based on configuration and a set of ien's that resolve the parent
        /// </summary>
        /// <param name="queryNode">The node which corresponds to this query configuration</param>
        /// <param name="query">The query to execute against the parent IENS</param>
        /// <param name="allIens">The list of IENS to query against</param>
        /// <param name="api">The api to query via</param>
        private void queryMultiple(TreeNode<QueryConfiguration> queryNode, VistaQuery query, ConcurrentQueue<string> allIens /*, ToolsApi api */)
        {
        //    // Protect
        //    if (query == null || String.IsNullOrEmpty(query.Fields) || String.IsNullOrEmpty(query.VistaFile) ||
        //        String.IsNullOrEmpty(query.StartIen) || String.IsNullOrEmpty(query.MaxRecords))
        //    {
        //        throw new ApplicationException("The query has not been setup properly");
        //    }

        //    // Get our data table
        //    DataTable results = queryNode.Value.DataTable;
        //    DataTable vertResults = queryNode.Value.GetsDataTable;

        //    // Set up our GETS word processing or computed fields variables
        //    bool hasWPOrComputedFields = false;
        //    bool hasGetsVertical = false;
        //    int wpOrComputedFieldsCount = 0;
        //    if (query.WP_Or_Computed_Fields == null || query.WP_Or_Computed_Fields.Equals(String.Empty))
        //    {
        //    }
        //    else
        //    {
        //        hasWPOrComputedFields = true;
        //        wpOrComputedFieldsCount = query.WP_Or_Computed_Fields.Split(new char[] { ';' }).Length;
        //        hasGetsVertical = (query.Gets_Alignment == null || query.Gets_Alignment.Equals(String.Empty)) ? false : true;
        //    }

        //    // Set up our collections to store the IENS which match the identifier being used
        //    ConcurrentQueue<string>[] queues = buildIENQueues(query.IdentifiedFiles);

        //    // Prepare to thread our subfile queries
        //    bool threadCritical = false;
        //    List<Task> tasks = new List<Task>();
        //    SemaphoreSlim dataTableProtection = new SemaphoreSlim(1, 1);
        //    bool holdingSemaphore = false;
        //    int workersToCreate = (allIens.Count < 8 /* _connectionPool.Count */) ? allIens.Count : 8 /* _connectionPool.Count */;

        //    // For each connection available, create a thread to work on these queries
        //    for (int i = 0; i < workersToCreate; ++i)
        //    {
        //        // Thread out our subqueries
        //        var task = Task.Factory.StartNew(() =>
        //        {
        //            // Set up
        //            string copy_ien = String.Empty;
        //            //AbstractConnection topLevelConn = null;
        //            //_connectionPool.TryDequeue(out topLevelConn);

        //            // Only allow subfile queries to fail so many times before aborting
        //            int subfileFailures = 0;

        //            // All of the workers will consume iens from the same concurrent collection
        //            while (allIens.TryDequeue(out copy_ien))
        //            {
        //                try
        //                {
        //                    // Perform our subfile query
        //                    string[] ddrResults = null;
        //                    ddrResults = _svc.ddrLister(query.SiteCode, query.VistaFile, String.Concat(",", copy_ien, ","), query.Fields, query.Flags, "", query.From, query.Part, query.XREF, query.Screen, query.Identifier);
        //                    //ddrResults = api.ddrLister
        //                    //(
        //                    //    topLevelConn,
        //                    //    query.VistaFile,
        //                    //    "," + copy_ien + ",",
        //                    //    query.Fields,
        //                    //    query.Flags,
        //                    //    String.Empty,
        //                    //    query.From,
        //                    //    query.Part,
        //                    //    query.XREF,
        //                    //    query.Screen,
        //                    //    query.Identifier
        //                    //);

        //                    // If this query didn't give us anything, move on to the next
        //                    if (ddrResults == null || ddrResults.Length == 0)
        //                    {
        //                        continue;
        //                    }

        //                    // Prepare to process our results
        //                    Int16 intSiteCode = Convert.ToInt16(query.SiteCode);
        //                    DateTime retrievalTime = DateTime.Now;

        //                    // Push each valid ddrresult into our datatable
        //                    foreach (string ddrResult in ddrResults)
        //                    {
        //                        // Break our string up into it's parts
        //                        string[] ddrValues = ddrResult.Replace("&#94;", "^").Split(new char[] { '^' });

        //                        // Capture and validate the IEN for this entry
        //                        decimal ien = 0;
        //                        if (!Decimal.TryParse(ddrValues[0], out ien))
        //                        {
        //                            continue;
        //                        }
        //                        if (ien == 0)
        //                        {
        //                            continue;
        //                        }

        //                        // package this record and add it to our table
        //                        string underscoreIens = copy_ien.Replace(",", "_");

        //                        // If this query wants a word processing or computed field, go get that now
        //                        if (hasWPOrComputedFields)
        //                        {
        //                            if (hasGetsVertical)
        //                            {
        //                                // Get the value of the computed fields
        //                                string[] getsResults = _svc.ddrGetsEntry(query.SiteCode, query.VistaFile, String.Concat(ien, ",", copy_ien, ","), query.WP_Or_Computed_Fields, "IN");
        //                                //string[] getsResults = api.ddrGetsEntry(topLevelConn, query.VistaFile, ien + "," + copy_ien + ",", query.WP_Or_Computed_Fields, "IN");

        //                                if (dataTableProtection.Wait(60 * 1000))
        //                                {
        //                                    holdingSemaphore = true;

        //                                    // Package the gets results vertically
        //                                    packageGetsQueryResultsVertically(
        //                                        underscoreIens,
        //                                        ien + "_" + underscoreIens + "",
        //                                        intSiteCode,
        //                                        retrievalTime,
        //                                        processGetsEntryResults(getsResults),
        //                                        vertResults);

        //                                    // Package everything else normally
        //                                    results.Rows.Add(
        //                                        packageVistaQueryResults(
        //                                            results.Columns.Count,
        //                                            queues.Length,
        //                                            ddrValues,
        //                                            underscoreIens,
        //                                            ien + "_" + underscoreIens + "",
        //                                            intSiteCode,
        //                                            retrievalTime,
        //                                            queues,
        //                                            emptyStringArray));
        //                                    dataTableProtection.Release();
        //                                    holdingSemaphore = false;
        //                                }
        //                                else
        //                                {
        //                                    throw new Exception("Unable to enter semaphore.");
        //                                }
        //                            }
        //                            else
        //                            {
        //                                // Get the value of the computed fields
        //                                string[] getsResults = _svc.ddrGetsEntry(query.SiteCode, query.VistaFile, String.Concat(ien, ",", copy_ien, ","), query.WP_Or_Computed_Fields, "I");
        //                                //string[] getsResults = api.ddrGetsEntry(topLevelConn, query.VistaFile, ien + "," + copy_ien + ",", query.WP_Or_Computed_Fields, "I");

        //                                // Masasage the data a bit
        //                                getsResults = processGetsEntryResults(getsResults, wpOrComputedFieldsCount);

        //                                // Package this record and add it to our table
        //                                if (dataTableProtection.Wait(60 * 1000))
        //                                {
        //                                    holdingSemaphore = true;
        //                                    results.Rows.Add(
        //                                        packageVistaQueryResults(
        //                                            results.Columns.Count,
        //                                            queues.Length,
        //                                            ddrValues,
        //                                            underscoreIens,
        //                                            ien + "_" + underscoreIens + "",
        //                                            intSiteCode,
        //                                            retrievalTime,
        //                                            queues,
        //                                            getsResults));
        //                                    dataTableProtection.Release();
        //                                    holdingSemaphore = false;
        //                                }
        //                                else
        //                                {
        //                                    throw new Exception("Unable to enter semaphore.");
        //                                }
        //                            }
        //                        }
        //                        else
        //                        {
        //                            if (dataTableProtection.Wait(60 * 1000))
        //                            {
        //                                holdingSemaphore = true;
        //                                try
        //                                {
        //                                    results.Rows.Add(
        //                                        packageVistaQueryResults(
        //                                            results.Columns.Count,
        //                                            queues.Length,
        //                                            ddrValues,
        //                                            underscoreIens,
        //                                            ien + "_" + underscoreIens + "",
        //                                            intSiteCode,
        //                                            retrievalTime,
        //                                            queues,
        //                                            emptyStringArray));
        //                                }
        //                                catch (Exception)
        //                                {
        //                                    throw;
        //                                }
        //                                finally
        //                                {
        //                                    dataTableProtection.Release();
        //                                    holdingSemaphore = false;
        //                                }
        //                            }
        //                            else
        //                            {
        //                                throw new Exception("Unable to enter semaphore.");
        //                            }
        //                        }
        //                    }
        //                }
        //                catch (Exception ex)
        //                {
        //                    // report
        //                   // _report.addError(ex.Message, ex);
        //                    ++subfileFailures;

        //                    // release lock if held
        //                    if (holdingSemaphore)
        //                    {
        //                        dataTableProtection.Release();
        //                        holdingSemaphore = false;
        //                    }

        //                    //if (topLevelConn != null)
        //                    //{
        //                    //    // We've failed far too many times to continue trying
        //                    //    // Give up hope
        //                    //    if (subfileFailures > 5)
        //                    //    {
        //                    //        threadCritical = true;
        //                    //        if (topLevelConn != null && !_connectionPool.Contains(topLevelConn))
        //                    //        {
        //                    //            _connectionPool.Enqueue(topLevelConn);
        //                    //        }
        //                    //        break;
        //                    //    }
        //                    //    else
        //                    //    {
        //                    //        try
        //                    //        {
        //                    //            // Release this connection since they are usually disconnected automatically on errors
        //                    //           // topLevelConn.disconnect();
        //                    //        }
        //                    //        catch (Exception exc)
        //                    //        {
        //                    //            // do nothing, if the query failed hard enough, the disconnect happens automatically
        //                    //            // so this will always happen
        //                    //        }
        //                    //        try
        //                    //        {
        //                    //            // When queries fail hard, the connection is automatically disconnected
        //                    //            // Let's procure another one for this worker since we are still within our
        //                    //            // failure threshold
        //                    //            //_connectionPool.Enqueue(createConnection(query.SiteCode));
        //                    //            //_connectionPool.TryDequeue(out topLevelConn);
        //                    //        }
        //                    //        catch (Exception exc)
        //                    //        {
        //                    //            // This sucks rocks - it means this worker is unable to do anything
        //                    //            // The best we can do is hope the other workers get through the rest of the work
        //                    //            _report.addError(exc.Message, exc);
        //                    //            threadCritical = true;
        //                    //            break;
        //                    //        }
        //                    //    }
        //                    //}
        //                    //else
        //                    //{
        //                    //    _report.addError("The thread does not have a handle on a connection");
        //                    //    threadCritical = true;
        //                    //    break;
        //                    //}
        //                }
        //            }

        //            // Release our handle on this connection
        //            //if (topLevelConn != null && !_connectionPool.Contains(topLevelConn))
        //            //{
        //            //    _connectionPool.Enqueue(topLevelConn);
        //            //}

        //        });

        //        // Add our task to the queue so that we can cleanly finish
        //        tasks.Add(task);
        //    }

        //    // Pause current thread executing until our tasks are completed
        //    bool done = false;
        //    Task.Factory.ContinueWhenAll(
        //        tasks.ToArray(), finalized =>
        //        {
        //            done = true;
        //        });

        //    while (!done)
        //    {
        //        Thread.Sleep(1000);
        //    }

        //    // Make sure none of our children went critical. We wait until now to throw the exception to the caller
        //    // beacuse if we call in the thread itself, the other worker threads will still be processing work
        //    // and when the parent asks the process to close it's connections, the pool will not have access
        //    // to them. This could result in unclosed site connections
        //    if (threadCritical)
        //    {
        //        throw new ApplicationException("One or more of the child threads encountered unrecoverable errors");
        //    }

        //    // For each of this nodes children, recursively evaluate them against each IEN with data
        //    // Our queues are populated in backward order, reverse the children and evaluate
        //    IEnumerable<TreeNode<QueryConfiguration>> children = queryNode.Children.Reverse();
        //    for (int i = 0; i < children.Count(); ++i)
        //    {
        //        // Only bother recursing for this file if we found IENS that have data for this file
        //        if (queues[i].Count > 0)
        //        {
        //            TreeNode<QueryConfiguration> childNode = children.ElementAt(i);
        //            VistaQuery childQuery = new VistaQuery(Configuration, childNode.Value);
        //            queryMultiple(childNode, childQuery, queues[i] /*, api */);
        //        }
        //    }

        }

        /// <summary>
        /// Collapse our tree-based datatable structure into a simple array
        /// </summary>
        /// <param name="table">The table to collapse into</param>
        /// <param name="node">The node to recurse against</param>
        /// <param name="place">The current index to insert into</param>
        private void collapseDataTables(DataTable[] table, TreeNode<QueryConfiguration> node)
        {
            if (r_depth == 0)
            {
                table[r_depth] = node.Value.DataTable;
                if (node.Value.GetsDataTable != null)
                {
                    table[++r_depth] = node.Value.GetsDataTable;
                }
            }
            foreach (TreeNode<QueryConfiguration> child in node.Children)
            {
                table[++r_depth] = child.Value.DataTable;
                if (child.Value.GetsDataTable != null)
                {
                    table[++r_depth] = child.Value.GetsDataTable;
                }
            }
            foreach (TreeNode<QueryConfiguration> child in node.Children)
            {
                collapseDataTables(table, child);
            }
        }

        /// <summary>
        /// Destroy our connection pool
        /// </summary>
        //public void destroyConnectionPool()
        //{
        //    DateTime start = DateTime.Now;
        //    AbstractConnection conn;
        //    UInt16 connectionsReleased = 0;
        //    while (_connectionPool.TryDequeue(out conn))
        //    {
        //        if (tearDownConnection(conn))
        //        {
        //            ++connectionsReleased;
        //        }
        //    }
        //    tearDownConnection();
        //    DateTime end = DateTime.Now;
        //    _report.addDebug("Succesfully tore down: " + connectionsReleased + " connection(s). Start:" + start + ", End:" + end);
        //}

        /// <summary>
        /// Disconnect a given connection and suppress any errors
        /// </summary>
        /// <param name="connection">The connection to disconnect</param>
        //private bool tearDownConnection(AbstractConnection connection)
        //{
        //    bool retVal = true;
        //    try
        //    {
        //        if (connection == null)
        //        {
        //            retVal = false;
        //        }
        //        connection.disconnect();
        //    }
        //    catch 
        //    {
        //        retVal = false;
        //    }
        //    return retVal;
        //}

        //private void tearDownConnection()
        //{
        //    try
        //    {
        //        if (_cxn != null && _cxn.IsConnected)
        //        {
        //            _cxn.disconnect();
        //        }
        //    }
        //    catch (Exception exc)
        //    {
        //        //LOG.Debug("An error occured while trying to disconnect from " + _cxn.SiteId.Key, exc);
        //    }
        //}

        /// <summary>
        /// Returns the number of records in the file as the key and the last IEN as the value for
        /// the Vista file specified in the VistaQuery argument
        /// </summary>
        /// <param name="query"></param>
        /// <returns></returns>
        public KeyValuePair<string, string> getRecordsInFileAndLastIen(VistaQuery query)
        {
            if (String.IsNullOrEmpty(query.Global) || !(query.Global.EndsWith("(") || query.Global.EndsWith(",")))
            {
                throw new ArgumentNullException("The VistaQuery argument must have a valid global set");
            }
            string global = "$G(" + query.Global + "0))";
            try
            {
                //setupConnection(query);

                string gvvResult = _svc.getVariableValueQuery(query.SiteCode, global);
                //ToolsApi toolsApi = new ToolsApi();
                // get number of records in file from header
                //string gvvResult = toolsApi.getVariableValue(query.Connection, global);
                string[] result = gvvResult.Split(new char[] { '^' });
                if (result == null || result.Length < 4)
                {
                    throw new FormatException("The data received was in an unexpected format!");
                }
                string numRex = result[3];
                // now get last IEN via DDR
                string[] ddrResult = _svc.ddrLister(query.SiteCode, query.VistaFile, "", ".01", "IPB", "1", "", "", "#", "", "");
                //string[] ddrResult = toolsApi.ddrLister(query.Connection, query.VistaFile, "", ".01", "IPB", "1", "", "", "#", "", "");
                if (ddrResult == null)
                {
                    throw new ApplicationException("No data returned from getLastVistaIen but no error was generated");
                }
                if (ddrResult.Length != 1 || String.IsNullOrEmpty(ddrResult[0]) || ddrResult[0].IndexOf('^') < 0)
                {
                    throw new FormatException("The data received was in an unexpected format!");
                }
                string lastIen = ddrResult[0].Substring(0, ddrResult[0].IndexOf('^'));

                return new KeyValuePair<string,string>(numRex, lastIen);
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                //tearDownConnection();
            }
        }

        /// <summary>
        /// Get the greatest record internal entry number for the Vista file specified in the VistaQuery argument
        /// </summary>
        /// <param name="query"></param>
        /// <returns></returns>
        public string getLastVistaIen(VistaQuery query)
        {
            try
            {
                //setupConnection(query);

                string[] result = _svc.ddrLister(query.SiteCode, query.VistaFile, "", ".01", "IPB", "1", "", "", "#", "", "");

               //ToolsApi toolsApi = new ToolsApi();
                //string[] result = toolsApi.ddrLister(query.Connection, query.VistaFile, "", ".01", "IPB", "1", "", "", "#", "", "");

                if (result == null)
                {
                    throw new ApplicationException("No data returned from getLastVistaIen but no error was generated");
                }
                if (result.Length != 1 || String.IsNullOrEmpty(result[0]) || result[0].IndexOf('^') < 0)
                {
                    throw new FormatException("The data received was in an unexpected format!");
                }
                return result[0].Substring(0, result[0].IndexOf('^'));
            }
            catch(Exception)
            {
                throw;
            }
            finally
            {
                //tearDownConnection();
            }
        }

        //public void createConnectionPool(String siteCode, UInt16 count)
        //{
        //    DateTime start = DateTime.Now;
        //    _connectionPool = new ConcurrentQueue<AbstractConnection>();
        //    UInt16 connectionsCreated = 0;

        //    for (int i = 0; i < count; ++i)
        //    {
        //        try
        //        {
        //            _connectionPool.Enqueue(createConnection(siteCode));
        //            ++connectionsCreated;
        //        }
        //        catch (Exception ex)
        //        {
        //            _report.addError(ex.Message, ex);
        //        }
        //    }

        //    // if we aren't able to create even a single connection, take a little break and try again a couple times before giving up
        //    if (_connectionPool.Count == 0)
        //    {
        //        _report.addDebug("Couldn't create a single connection. Sleeping and trying again...");
        //        int tries = 0;
        //        while (tries < 5)
        //        {
        //            ++tries;
        //            Thread.Sleep(1000 * 60 * 1);
        //            try
        //            {
        //                _connectionPool.Enqueue(createConnection(siteCode));
        //                ++connectionsCreated;
        //                break;
        //            }
        //            catch (Exception ex)
        //            {
        //                _report.addError(ex.Message, ex);
        //            }
        //        }
        //    }
        //    if (_connectionPool.Count == 0) throw new ApplicationException("Unable to procure a connection");

        //    DateTime end = DateTime.Now;
        //    _report.addDebug("Succesfully set up: " + connectionsCreated + " connection(s). Start:" + start + ", End:" + end);
        //}

        /// <summary>
        /// Create a simple connection for a given site
        /// </summary>
        /// <param name="siteCode">The Site to connect to</param>
        /// <returns>The new connection</returns>
        //internal AbstractConnection createConnection(String siteCode)
        //{
        //    AbstractConnection connection = null;
        //    try
        //    {
        //        // Set up
        //        Site site = _extractorSites.getSite(siteCode);

        //        DataSource src = site.getDataSourceByModality("HIS");
        //        connection = AbstractDaoFactory.getDaoFactory(AbstractDaoFactory.getConstant("VISTA")).getConnection(src); // new VistaConnection(src);
        //        connection.connect();
        //        connection.Account.AuthenticationMethod = VistaConstants.NON_BSE_CREDENTIALS;
        //        User user = connection.Account.authenticateAndAuthorize(_downstreamCreds, new MenuOption("DVBA CAPRI GUI"), null);

        //        // Protect
        //        if (user == null || connection == null)
        //        {
        //            throw new ApplicationException("An unexpected error occurred while attmepting to setup a connection to " + siteCode);   
        //        }
        //    }
        //    catch( Exception ex )
        //    {
        //        throw new ApplicationException( "Unable to create connection", ex );
        //    }
        //    return connection;
        //}

        //// setup connection using administrative credentials
        //internal void setupConnection(VistaQuery query)
        //{
        //    if (query == null || String.IsNullOrEmpty(query.SiteCode) || !_sitecodes.Contains(query.SiteCode))
        //    {
        //        throw new ApplicationException("Must supply a valid VistaQuery object!");
        //    }

        //    try
        //    {
        //        if (_cxn != null && _cxn.IsConnected)
        //        {
        //            return;
        //        }
        //        Site site = _extractorSites.getSite(query.SiteCode);
        //        DataSource src = site.getDataSourceByModality("HIS");
        //        _cxn = AbstractDaoFactory.getDaoFactory(AbstractDaoFactory.getConstant("VISTA")).getConnection(src); // new VistaConnection(src);
        //        _cxn.connect();
        //        _cxn.Account.AuthenticationMethod = VistaConstants.NON_BSE_CREDENTIALS;
        //        User user = _cxn.Account.authenticateAndAuthorize(_downstreamCreds, new MenuOption("DVBA CAPRI GUI"), null);
        //        query.Connection = _cxn;
                    
        //        if (user == null)
        //        {
        //            throw new ApplicationException("An unexpected error occurred while attmepting to setup a connection to " + query.Connection.DataSource.SiteId.Id);
        //        }
        //    }
        //    catch (Exception)
        //    {
        //        throw;
        //    }
        //}

        //internal void teardownConnection()
        //{
        //    if (_cxn == null)
        //    {
        //        return;
        //    }
        //    try
        //    {
        //        _cxn.disconnect();
        //    }
        //    catch (Exception) { }
        //}

        internal AbstractCredentials getDownstreamCredentials(User user)
        {
            AbstractCredentials result = new VistaCredentials();
            result.AuthenticationSource = new DataSource(); // BSE
            result.AuthenticationSource.SiteId = new SiteId(user.LogonSiteId.Id, user.LogonSiteId.Name);
            result.AuthenticationToken = user.LogonSiteId.Id + "_" + user.Uid;
            result.LocalUid = user.Uid;
            result.FederatedUid = user.SSN.toString();
            result.SubjectName = user.Name.getLastNameFirst();
            result.SubjectPhone = user.Phone;
            result.SecurityPhrase = VistaConstants.MY_SECURITY_PHRASE;
            return result;
        }

        internal User getDownstreamUser()
        {
            return null;
        }

        #region Setters and Getters

        private SiteTable Sites
        {
            get { return _extractorSites; }
            set { _extractorSites = value; }
        }

        public IList<string> SiteCodes
        {
            get { return _sitecodes; }
        }

        //public ExtractorReport Report
        //{
        //    get { return _report; }
        //    set { _report = value; }
        //}

        #endregion


    }
}
