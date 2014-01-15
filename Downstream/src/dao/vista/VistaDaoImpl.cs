using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections.Concurrent;
using com.bitscopic.downstream.domain;
using com.bitscopic.downstream.domain.exception;
using com.bitscopic.downstream.domain.reporting;
using com.bitscopic.downstream.utils;
using System.Data;
using System.Threading;
using System.Configuration;
using System.Threading.Tasks;

namespace com.bitscopic.downstream.dao.vista
{
    public class VistaDaoImpl
    {
        //Dictionary<String, String> _labChemDataDictionary;
        IList<String> _labChemIens;

        ThreadSafeReport _report = new domain.reporting.ThreadSafeReport();

        //ConcurrentDictionary<String, Dictionary<String, String>> _verticalResults;
        ConcurrentDictionary<String, String> _wpOrComputedResults;
        ConcurrentBag<QueryResults> _resultsBag;
        ConcurrentBag<Exception> _exceptionBag;
        ExtractorConfiguration _configForQueryWithDepth;

        // these setters and getters are for unit tests - don't use them elsewhere unless you know what you're doing
        internal ConcurrentBag<QueryResults> ResultsBag { get { return _resultsBag; } set { _resultsBag = value; } }
        internal ConcurrentBag<Exception> ExceptionBag { get { return _exceptionBag; } set { _exceptionBag = value; } }
        internal ExtractorConfiguration Config { get { return _configForQueryWithDepth; } set { _configForQueryWithDepth = value; } }

        Int16 _subQueryWorkers = 8; // default
        public Uri BaseUri { get; set; }

        IVistaDao _dao;

        public VistaDaoImpl()
        {
            _dao = new VistaDaoFactory().getVistaDao(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.VistaDaoType]);
            if (!String.IsNullOrEmpty(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SubQueryWorkers]))
            {
                Int16.TryParse(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SubQueryWorkers], out _subQueryWorkers);
            }
        }

        // used for unit tests or anywhere we want to manually set the DAO type
        internal void setDao(IVistaDao dao)
        {
            _dao = dao;
        }

        internal IVistaDao getDao()
        {
            return _dao;
        }

        internal Report getReport()
        {
            return _report;
        }

        public QueryResults queryWithDepth(ExtractorConfiguration config, VistaQuery topLevelQuery)
        {
            _configForQueryWithDepth = config;
            // IEN seeding
            VistaIenSeeder seeder = new VistaIenSeeder(_dao);
            if (String.Equals(topLevelQuery.From, "0") && seeder.needsSeeding(topLevelQuery.VistaFile)) // if we're starting at beginning of file and file is being seeded
            {
                _report.addDebug("This file requires IEN seeding!");
                topLevelQuery.From = seeder.getSeed(topLevelQuery.SiteCode, topLevelQuery.VistaFile);
                if (String.IsNullOrEmpty(topLevelQuery.From)) // some sites/files have no records - getSeed will return an empty string in those cases
                {
                    throw new NoDataInSiteAndFileException();
                }
            }
            // end IEN seeding
            // special DD stuff for 63 file tree
            if (LabChemUtils.containsLabChemConfig(config))
            {
                setupLabChem(config, topLevelQuery);
            }
            // end 63 file tree setup
            QueryResults topLevel = this.query(topLevelQuery);
            if (topLevel.SubQueryIens != null && topLevel.SubQueryIens.Count > 0)
            {
                foreach (String subFile in topLevel.SubQueryIens.Keys)
                {
                    ConcurrentQueue<VistaQuery> subFileQueryQueue = DataTableUtils.getSubQueriesFromResultsBySubfile(config, topLevel, subFile);
                    queryMultiple(config, subFileQueryQueue, topLevel);
                }
            }
            return topLevel;
        }

        internal void setupLabChem(ExtractorConfiguration config, VistaQuery topLevelQuery)
        {
            if (!(_dao is MdoVistaDao))
            {
                throw new ConfigurationException("File 63 tree with LAB CHEM can only be extracted with a local MdoVistaDao Vista DAO impl configuration");
            }

            //_labChemDataDictionary = VistaDaoUtils.getLabChemFieldsDynamic(topLevelQuery.SiteCode,
            //    (gov.va.medora.mdo.dao.AbstractConnection)gov.va.medora.mdo.domain.pool.connection.ConnectionPools.getInstance().checkOutAlive(topLevelQuery.SiteCode));
            _labChemIens = new List<String>(); // use this to keep track of file 63 IENs that have data in 63.04 for purposes of ticket #76
        }

        public void queryMultiple(ExtractorConfiguration config, ConcurrentQueue<VistaQuery> queriesToExecute, QueryResults topLevelResults)
        {
            while (queriesToExecute.Count > 0)
            {
                bool runningLabChem = false;
                if (queriesToExecute != null && queriesToExecute.Count > 0 && String.Equals(queriesToExecute.First().VistaFile, "63.04"))
                {
                    runningLabChem = true; // only set this true when we are actually executing lab chem queries
                }
                // first setup table to combine all results from multi-threaded queries
                VistaQuery trash = null;
                queriesToExecute.TryPeek(out trash);
                DataTable subfileTable = DataTableUtils.generateVistaQueryDataTable(
                    trash.VistaFile, trash.Fields.Split(new char[] { ';' }), true, trash.WP_Or_Computed_Fields.Split(new char[] { ';' }));
                // now multi-threaded query
                query(queriesToExecute);

                Dictionary<String, ConcurrentQueue<VistaQuery>> levelQueues = new Dictionary<string, ConcurrentQueue<VistaQuery>>();
                // now we need to go through each query result and add the IENS to the level queue
                foreach (QueryResults result in _resultsBag)
                {
                    if (runningLabChem)
                    {
                        continue; // nothing to do here - just continue
                    }
                    // we didn't receive any results for this subfile even though the parent query indicated there was data - TBD - should we log???
                    if (result.DdrResults[0].Rows.Count <= 0)
                    {
                        _exceptionBag.Add(new domain.exception.DownstreamException(String.Format("No results found when querying a subfile despite indicator in parent file {0}", result.DdrResults[0].TableName)));
                        continue;
                    }
                    if (result.SubQueryIens == null)
                    {
                        continue;
                    }
                    foreach (String key in result.SubQueryIens.Keys)
                    {
                        if (!levelQueues.ContainsKey(key)) // we can't set this up before entering the loop because not all query results will have data/sub-query IENS
                        {
                            levelQueues.Add(key, new ConcurrentQueue<VistaQuery>());
                        }
                        ConcurrentQueue<VistaQuery> subQueries = DataTableUtils.getSubQueriesFromResultsBySubfile(config, result, key);
                        foreach(VistaQuery subQuery in subQueries)
                        {
                            levelQueues[key].Enqueue(subQuery);
                        }
                    }
                }
                // done setting up level queues

                // special 63.04 handling
                if (runningLabChem)
                {
                    StringBuilder combined63x04Data = new StringBuilder();
                    foreach (QueryResults result in _resultsBag)
                    {
                        combined63x04Data.Append(result.StringResult);
                    }
                    topLevelResults.StringResult = combined63x04Data.ToString();
                    String[] temp = new String[_labChemIens.Count];
                    _labChemIens.CopyTo(temp, 0);
                    topLevelResults.LabChemIens = temp.ToList();
                    _labChemIens = null;
                    return; // don't want to run through code below!
                }

                // end special 63.04 handling

                foreach (QueryResults result in _resultsBag)
                {
                    DataTableUtils.addResultsToTable(subfileTable, result.DdrResults[0]);
                }
                foreach (Exception error in _exceptionBag)
                {
                    _report.addException(error);
                    _report.addDebug("Found " + _exceptionBag.Count + " various errors while querying subfiles for this batch");
                }

                // we might have an _KEYVALUE tables in our collection of results - check for those and, if present, combine them all in one table
                consolidateKeyValueTables(topLevelResults, trash);
                
                // then add our table to top level (but only if we have records)
                if (subfileTable != null && subfileTable.Rows.Count > 0)
                {
                    topLevelResults.DdrResults.Add(subfileTable);
                }

                // let's log a summary of this level query
                _report.addInfo("Found " + subfileTable.Rows.Count + " records for subfile " + subfileTable.TableName);

                // finally recurse through subfiles of this subfile
                foreach (String subFile in levelQueues.Keys)
                {
                    if (levelQueues[subFile] != null && levelQueues[subFile].Count > 0)
                    {
                        _report.addInfo("Starting query for " + levelQueues[subFile].Count + " subfile records for file " + subFile + " based on identifier results from parent query");
                    }
                    else
                    {
                        _report.addInfo(subFile + " does not appear to have any data for this IEN range");
                    }
                    queryMultiple(config, levelQueues[subFile], topLevelResults);
                }
            }
        }

        void consolidateKeyValueTables(QueryResults topLevelResults, VistaQuery query)
        {
            // we might have an _KEYVALUE tables in our collection of results - check for those and, if present, combine them all in one table
            if (!String.IsNullOrEmpty(query.WP_Or_Computed_Fields) && !String.IsNullOrEmpty(query.Gets_Alignment))
            {
                // ok - so we know the configuration specified we should query to create a KEYVALUE table - did we get any results?
                bool foundOne = false;
                foreach (QueryResults result in _resultsBag)
                {
                    if (result.DdrResults.Count > 1) // when the config specifies _KEYVALUE tables, we will have more than one DataTable in our QueryResults
                    {
                        foundOne = true;
                        break;
                    }
                }
                if (foundOne)
                {
                    DataTable containerForAll = DataTableUtils.generateVerticalDataTable(query.VistaFile, true);
                    topLevelResults.DdrResults.Add(containerForAll);
                    foreach (QueryResults result in _resultsBag)
                    {
                        if (result.DdrResults.Count > 1 && result.DdrResults[1] != null && result.DdrResults[1].Rows.Count > 0)
                        {
                            DataTableUtils.addResultsToTable(containerForAll, result.DdrResults[1]);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Use this method to create a bunch of workers for the passed VistaQuery collection.
        /// The threads will populate this class' ConcurrentBag of QueryResults and Exception
        /// These concurrent collections will need to be merged after this function returns
        /// </summary>
        /// <param name="queries"></param>
        internal void query(ConcurrentQueue<VistaQuery> queriesQueue)
        {
            _resultsBag = new ConcurrentBag<QueryResults>();
            _exceptionBag = new ConcurrentBag<Exception>();

            IList<Task> workerTasks = new List<Task>();
            for (int i = 0; i < _subQueryWorkers; i++)
            {
                Task newTask = new Task(() => threadedQuery(queriesQueue));
                workerTasks.Add(newTask);
                newTask.Start();
            }

            foreach (Task t in workerTasks)
            {
                t.Wait();
            }
        }

        internal void threadedQuery(ConcurrentQueue<VistaQuery> concurrentQueueOfQueries)
        {
            //ConcurrentQueue<VistaQuery> queue = (ConcurrentQueue<VistaQuery>)concurrentQueueOfQueries;
            VistaQuery currentQuery = null;
            while (concurrentQueueOfQueries.TryDequeue(out currentQuery))
            {
                try
                {
                    QueryResults result = query(currentQuery);
                    _resultsBag.Add(result);
                }
                catch (Exception exc)
                {
                    _exceptionBag.Add(new downstream.domain.exception.DownstreamException("Problem with query:" + Environment.NewLine + currentQuery.ToString() + exc.ToString()));
                    try
                    {
                        if (currentQuery.QueryErrorCount < 3)
                        {
                            currentQuery.QueryErrorCount++;
                            concurrentQueueOfQueries.Enqueue(currentQuery); // re-queue if error but don't try forever - probably a unrecoverable issue if multiple failures on same query
                        }
                    }
                    catch (Exception) { /* guess have no choice but to swallow... */ }
                }
            }
        }

        public QueryResults query(VistaQuery query)
        {
            // if we're querying unpacked, we should add the "WID" field to our fields string so DDR LISTER will return the identifier values
            String[] ddrResults = _dao.ddrLister(query.SiteCode, query.VistaFile, query.IENS, query.Fields, query.Flags, query.MaxRecords, query.From, query.Part, query.XREF, query.Screen, query.Identifier);

            // special 63.04 handling
            if (String.Equals(query.VistaFile, "63.04"))
            {
                _labChemIens.Add(query.IENS.Replace(",", "")); // add the file 63 IEN to this collection for ticket #76
                //return new LabChemUtils(_labChemDataDictionary).parseLabChemDdrResults(query, ddrResults);
                return new LabChemUtils().parseLabChemDdrResults(query, ddrResults);
            }
            // end special 63.04 handling

            // first check to see if we should fetch key/val (aka vertical) results
            DataTable verticalResults = null;
            if (!String.IsNullOrEmpty(query.WP_Or_Computed_Fields) && !String.IsNullOrEmpty(query.Gets_Alignment)) // KEY/VAL SUB QUERIES
            {
                verticalResults = getVerticalResultsForQueries(query, ddrResults, query.IENS);
            } // if not looking for vertical, do we need to fetch WP or other large fields and add them to DDR?
            else if (!String.IsNullOrEmpty(query.WP_Or_Computed_Fields)) // WP fields
            {
                ddrResults = addWpOrComputed(query, ddrResults);
            }

            QueryResults qr = null;
            // if we are fetching WP fields AND this configuration isn't building a key value table AND there are subfiles then we need to call our super special method in DataTableUtils! ticket #16
            if (!String.IsNullOrEmpty(query.WP_Or_Computed_Fields) && String.IsNullOrEmpty(query.Gets_Alignment) && !String.IsNullOrEmpty(query.IdentifiedFiles))
            {
                DataTableUtils.adjustDdrResultsWithWpAndIdentifiedFiles(query, ddrResults);
                qr = DataTableUtils.toQueryResultsFromDdr(query, ddrResults); // ddrResults is "fixed" by adjust function
            }
            else // for most cases, just building this up without special logic above
            {
                qr = DataTableUtils.toQueryResultsFromDdr(query, ddrResults);
            }

            // ugh - this seems ugly and hackish doing this out of process from the subqueries for WP fields above... oh, well, seems ok for now at least
            if (!String.IsNullOrEmpty(query.WP_Or_Computed_Fields) && _exceptionBag != null)
            {
                foreach (Exception e in _exceptionBag)
                {
                    _report.Exceptions.Add(e);
                }
            }

            // did we have any key/val queries? if so, add the table to our results
            if (verticalResults != null)
            {
                qr.DdrResults.Add(verticalResults);
            }

            return qr; // DataTableUtils.toQueryResultsFromDdr(query, ddrResults);
        }

        // currently this is only being used for file 63.3 - because the results are nested and 63.3 doesn't typically contain
        // a lot of records in the subfile path, i'm removing all the multithreading crap since it caused an issue with the extracted data
        internal DataTable getVerticalResultsForQueries(VistaQuery topLevelQuery, String[] ddrResults, String parentRecordIENS)
        {
            DataTable verticalResults = DataTableUtils.generateVerticalDataTable(topLevelQuery.VistaFile, topLevelQuery.IsSubFileQuery);
            if (ddrResults == null || ddrResults.Length == 0)
            {
                return verticalResults;
            }

            // build queue of queries and prepare threadsafe placeholder for results
           // _verticalResults = new ConcurrentDictionary<string, Dictionary<string, string>>();

            Dictionary<String, Dictionary<String, String>> resultsDict = new Dictionary<String, Dictionary<String, String>>();
            ConcurrentQueue<VistaQuery> verticalQueries = new ConcurrentQueue<VistaQuery>();
            foreach (String ddrResult in ddrResults)
            {
                String correctedIensString = parentRecordIENS;
                if (!String.IsNullOrEmpty(correctedIensString))
                {
                    if (correctedIensString.StartsWith(","))
                    {
                        correctedIensString = correctedIensString.Substring(1, correctedIensString.Length - 1); // remove leading comma so IENS string below is builts correctly
                    }
                }
                correctedIensString = String.Concat(ddrResult.Split(new char[] { '^' })[0], ",", correctedIensString);// IEN from current ddr result + , + IENS string from top level query

                VistaQuery vq = new VistaQuery()
                {
                    Fields = topLevelQuery.WP_Or_Computed_Fields,
                    Flags = "IN",
                    IENS = correctedIensString,
                    SiteCode = topLevelQuery.SiteCode,
                    VistaFile = topLevelQuery.VistaFile, // same vista file as specified in top level query
                };

                Dictionary<String, String> result = VistaDaoUtils.toDictFromDdrGetsEntry(_dao.ddrGetsEntry(vq.SiteCode, vq.VistaFile, vq.IENS, vq.Fields, vq.Flags));

                resultsDict.Add(correctedIensString, result);
              //  verticalQueries.Enqueue(vq);

             //   _verticalResults.TryAdd(correctedIensString, new Dictionary<string, string>()); // we'll have multiple results per IENS
            }

            // multi-thread queries
            //IList<Task> verticalTasks = new List<Task>();
            //for (int i = 0; i < _subQueryWorkers; i++)
            //{
            //    Task workerTask = new Task(() => queryKeyValue(verticalQueries));
            //    verticalTasks.Add(workerTask);
            //    workerTask.Start();
            //}

            //foreach (Task t in verticalTasks)
            //{
            //    t.Wait();
            //}

            // put all our results in to a single data table
            DateTime batchTimestamp = DateTime.Now; // we give all our Key/Val the same timestamp
            foreach (String key in resultsDict.Keys)
            {
                // DataTableUtils.toKeyValTableFromDdrGetsEntry only needs the subfile info, IENS, sitecode, and timestamp
                VistaQuery reconstructedQuery = new VistaQuery()
                {
                    IENS = key,
                    SiteCode = topLevelQuery.SiteCode,
                    IsSubFileQuery = topLevelQuery.IsSubFileQuery
                };
                DataTable currentBatch = DataTableUtils.toKeyValTableFromDdrGetsEntry(reconstructedQuery, resultsDict[key], batchTimestamp);
                DataTableUtils.addResultsToTable(verticalResults, currentBatch);
            }

            // done! return vertical/key-val datatable
            return verticalResults;
        }

        //void queryKeyValue(ConcurrentQueue<VistaQuery> queries, ConcurrentDictionary<String, Dictionary<String, String>> resultsBag)
        //{
        //    //ConcurrentQueue<VistaQuery> jobs = (ConcurrentQueue<VistaQuery>)queries;

        //    VistaQuery current = null;
        //    while (queries.TryDequeue(out current))
        //    {
        //        try
        //        {
        //            Dictionary<String, String> keyValResult = VistaDaoUtils.toDictFromDdrGetsEntry(_dao.ddrGetsEntry(current.SiteCode, current.VistaFile, current.IENS, current.Fields, current.Flags));
        //            resultsBag[current.IENS] = keyValResult;
        //        }
        //        catch (Exception)
        //        {
        //            // todo - create placeholder for exceptions!
        //        }
        //    }
        //}

        /// <summary>
        /// Threaded WP query function
        /// </summary>
        /// <param name="allQueries">ConcurrentQueue of VistaQuery</param>
        void queryWpOrComputed(ConcurrentQueue<VistaQuery> allQueries)
        {
            //ConcurrentQueue<VistaQuery> all = (ConcurrentQueue<VistaQuery>)allQueries;

            VistaQuery myQuery = null;
            while (allQueries.TryDequeue(out myQuery))
            {
                try
                {
                    String[] result = _dao.ddrGetsEntry(myQuery.SiteCode, myQuery.VistaFile, myQuery.IENS, myQuery.Fields, myQuery.Flags);
                    String adjustedResult = DataTableUtils.addGetsEntryToDdrResult(myQuery.From, result, myQuery);
                    _wpOrComputedResults.TryAdd(myQuery.IENS, adjustedResult);
                }
                catch (Exception exc)
                {
                    _exceptionBag.Add(exc);
                    try
                    {
                        if (myQuery.QueryErrorCount < 3)
                        {
                            myQuery.QueryErrorCount++;
                            allQueries.Enqueue(myQuery); // re-queue if error but don't try forever - probably a unrecoverable issue if multiple failures on same query
                        }
                    }
                    catch (Exception) { /* guess have no choice but to swallow... */ }
                }
            }
        }

        public String[] addWpOrComputed(VistaQuery topLevelQuery, String[] ddrResults)
        {
            // create queue of queries
            ConcurrentQueue<VistaQuery> wpOrComputedQueries = new ConcurrentQueue<VistaQuery>();
            IList<String> iensStrings = new List<String>(); // we'll use this as a holding location for all our queries so we don't have to build the IENS string twice
            foreach (String ddrResult in ddrResults)
            {
                String correctedIensString = topLevelQuery.IENS;
                if (!String.IsNullOrEmpty(correctedIensString))
                {
                    if (correctedIensString.StartsWith(","))
                    {
                        correctedIensString = correctedIensString.Substring(1, correctedIensString.Length - 1); // remove leading comma so IENS string below is builts correctly
                    }
                }
                correctedIensString = String.Concat(ddrResult.Split(new char[] { '^' })[0], ",", correctedIensString);// IEN from current ddr result + , + IENS string from top level query
                iensStrings.Add(correctedIensString);
                VistaQuery wpOrComputedQuery = new VistaQuery()
                {
                    WP_Or_Computed_Fields = topLevelQuery.WP_Or_Computed_Fields, // we copy this because DataTableUtils.addGetsEntryToDdrResult looks for it there!!!
                    Fields = topLevelQuery.WP_Or_Computed_Fields,
                    VistaFile = topLevelQuery.VistaFile,
                    SiteCode = topLevelQuery.SiteCode,
                    IENS = correctedIensString, 
                    Flags = "I",
                    From = ddrResult // NOTE - we're hijacking this field since it's not used in the DDR GETS ENTRY call to store the current DDR result for the multi-threaded queries
                };
                wpOrComputedQueries.Enqueue(wpOrComputedQuery);
            }

            _exceptionBag = new ConcurrentBag<Exception>();
            _wpOrComputedResults = new ConcurrentDictionary<string, string>(); // create new before each kickoff

            IList<Task> wpOrComputedTasks = new List<Task>();
            //IList<Thread> wpOrComputedThreads = new List<Thread>();
            for (int i = 0; i < _subQueryWorkers; i++)
            {
                Task wpOrComputedTask = new Task(() => queryWpOrComputed(wpOrComputedQueries));
                wpOrComputedTasks.Add(wpOrComputedTask);
                wpOrComputedTask.Start();
                //Thread wpOrComputedThread = new Thread(new ParameterizedThreadStart(this.queryWpOrComputed));
                //wpOrComputedThreads.Add(wpOrComputedThread);
                //wpOrComputedThread.Start(wpOrComputedQueries);
            }

            foreach (Task t in wpOrComputedTasks)
            {
                t.Wait();
            }
            //foreach (Thread t in wpOrComputedThreads)
            //{
            //    t.Join();
            //}

            for (int i = 0; i < iensStrings.Count; i++ ) // go through each IENS string and get value from subqueries dictionary
            {
                if (_wpOrComputedResults.ContainsKey(iensStrings[i]))
                {
                    ddrResults[i] = _wpOrComputedResults[iensStrings[i]]; // replace DDR result with corrected - we do it this way to preserve the order of the original DDR query results
                }
            }
            return ddrResults;
        }

    }
}
