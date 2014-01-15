using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Configuration;
using com.bitscopic.downstream.dao.file;
using com.bitscopic.downstream.dao.vista;
using com.bitscopic.downstream.domain;
using com.bitscopic.downstream.domain.exception;
using com.bitscopic.downstream.domain.reporting;
using com.bitscopic.downstream.net;
using com.bitscopic.downstream.utils;
using gov.va.medora.mdo;
using System.Threading;
using System.Data;
using System.Net;
using System.IO;
using com.bitscopic.downstream.compression.gzip;
using com.bitscopic.downstream.net.utils;

namespace com.bitscopic.downstream.service
{
    public class VistaService : AbstractService
    {
        DateTime _serviceStart = DateTime.Now;
        TimeSpan _maxRunTime = new TimeSpan(5, 0, 0, 0); // default this to 5 days - we never want any extractions to run longer
        VistaServiceState _serviceState;
        VistaDaoImpl _vistaDao;
        //dao.vista.IVistaDao _vistaDao;
        IFileDao _fileDao; // only want one of these for this class - has instance specific functions
        ExtractorConfiguration _config;
        Extractor _extractor;
        VistaQuery _vistaQuery;
        Server _server;
        Client _client;
        // internals for unit tests
        internal string HostName { get { return _server.SocketContainer.HostName; } }
        internal Int32 Port { get { return _server.SocketContainer.ListeningPort; } }
        //string _lastSqlIEN;
        string _currentExtractorIEN;
        ExtractorReport _report = new ExtractorReport("Not yet assigned"); // don't have batch ID yet - set it below
        public ExtractorReport Report { get { return _report; } set { _report = value; } }
        bool _incrementalUploads = false;
        bool _logSubqueryMessages = true;

        public VistaService() { }

        internal override void shutdown()
        {
            stop();
        }

        protected override void start()
        {
            setUp();
            _client = new Client();
            _server = new Server(true);
            _server.SocketContainer.ServiceState = _serviceState;
            try
            {
                // start server first since newJobRequest ensures local host is reachable
                _server.startListener();
                _report.ExtractorHostName = _server.SocketContainer.HostName;
            }
            catch (Exception)
            {
                _report.addError("Unable to start listener for client! Unrecoverable error so exiting...");
                throw; // unable to start server - can't do anything!!!
            }
            try
            {
                //_client.connect(ConfigurationManager.AppSettings["OrchestratorHostName"],
                //            Convert.ToInt32(ConfigurationManager.AppSettings["ServerListeningPort"]));
            }
            catch (Exception)
            {
                _report.addError("Unable to connect to orchestrator on " +
                    ConfigurationManager.AppSettings["OrchestratorHostName"] + ":" +
                    ConfigurationManager.AppSettings["ServerListeningPort"] +
                    " - Unrecoverable error so exiting...");
                throw; // unable to start service - can't do anything!!!
            }
        }
        protected override void stop()
        {
            _server.stopListener();
            this.setServiceState(ServiceStatus.STOPPED);
        }

        private void setUp()
        {
            _serviceStart = DateTime.Now;
            if (!String.IsNullOrEmpty(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.MaxRunTime]))
            {
                TimeSpan.TryParse(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.MaxRunTime], out _maxRunTime);
            }
            _serviceState = new VistaServiceState();
            _vistaDao = new VistaDaoImpl(); // impl sets DAO type based off config -> new VistaDaoFactory().getVistaDao(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.VistaDaoType]);
            //_lastSqlIEN = 
            _currentExtractorIEN = "0";
            _config = null;

            if (ConfigurationManager.AppSettings.AllKeys != null && ConfigurationManager.AppSettings.AllKeys.Contains("Email"))
            {
                _report.addInfo("Extractor contact: " + ConfigurationManager.AppSettings["Email"]);
            }

            // try to get this from config - if it's not present, no worries - defaults to false which is ok!
            Boolean.TryParse(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.IncrementalFileUploads], out _incrementalUploads);
            Boolean.TryParse(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.IncludeSubfileLogs], out _logSubqueryMessages);

            _report.addDebug("Setup complete for Vista service!");
        }

        protected override void run()
        {
            try
            {
                MessageTO jobResponse = null;
                if (String.Equals("false", ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.EnforceCallback], StringComparison.CurrentCultureIgnoreCase))
                {
                    jobResponse = _client.sendNewJobRequest("SansCallback", 0);
                }
                else // default if config item doesn't exist or it set to anything but false
                {
                    jobResponse = _client.sendNewJobRequest(_server.SocketContainer.HostName, _server.SocketContainer.ListeningPort);
                }
                _client.disconnect();
                // check job stack isn't empty
                if (jobResponse != null && jobResponse.MessageType == MessageTypeTO.NewJobResponse &&
                    !String.IsNullOrEmpty(jobResponse.Message) && jobResponse.Message.Contains("No more jobs"))
                {
                    _report.addInfo("The Orchestrator service reported there are no more jobs on the work stack. Exiting normally...");
                    return;
                }
                // then check we received a valid response
                if (jobResponse == null || jobResponse.MessageType != MessageTypeTO.NewJobResponse ||
                    jobResponse.Configuration == null || !jobResponse.Configuration.isCompleteConfiguration())
                {
                    throw new DownstreamException(null, null, null, jobResponse, "Invalid new job response!", null);
                }

                _report.addInfo("Successfully connected and registered with orchestrator and received a valid job!");
                _config = jobResponse.Configuration;
                _report.setConfiguration(_config);
                _report.BatchId = jobResponse.Message; // we have the batch ID now - set it on the report for easy tracing
                _extractor = jobResponse.Extractor;

                // now that we have our config, we can create our file DAO
                _fileDao = new FileDaoFactory().getFileDao();
                _fileDao.setExtractsDirectory(_config.SiteCode, jobResponse.Message); // messsage should contain correct directory name


                // Create the top level query that will drive any subqueries
                _vistaQuery = new VistaQuery(_config, _config.QueryConfigurations.RootNode.Value);

                if (checkSiteForWork())
                {
                    _report.addDebug(_config.SiteCode + " appears to have work! Starting extraction job...");
                    extract();
                }
                else
                {
                    _report.addDebug("Site appears to have no work!");
                }

                // finally, try and notify orchestrator we have finished
                sendUnlockRequest(MessageTypeTO.JobCompletedRequest, _client);
            }
            catch (Exception exc)
            {
                _report.Errored = true;
                _report.addException(exc);
                if (_config != null) // if this was set then we know we received a job
                {
                    sendUnlockRequest(MessageTypeTO.JobErrorRequest, _client);
                }
            }
            finally
            {
                try
                {
                    _client.disconnect();
                    _server.stopListener();
                }
                catch (Exception) { /* nothing we can do if these error */ }
                GC.Collect();
            }
        }


        void sendUnlockRequest(MessageTypeTO unlockType, Client client)
        {
            if (_config != null)
            {
                _report.addDebug("Getting ready to try sending an unlock request of type " + Enum.GetName(typeof(MessageTypeTO), unlockType) +
                    " for the following extraction configuration: " + _config.ToString());
            }
            bool markedCompleted = false;
            int tries = 0;
            int maxTries = 60;
            _report.EndTimestamp = DateTime.Now;
            if (unlockType == MessageTypeTO.JobErrorRequest)
            {
                maxTries = 3;
            }
            while (!markedCompleted)
            {
                if (tries > maxTries) // can't really wait forever - wait about 1 hour then quit
                {
                    break;
                }
                tries++;
                try
                {
                    if (unlockType == MessageTypeTO.JobCompletedRequest)
                    {

                        if (client.sendJobCompletedRequest(_config, _report, _currentExtractorIEN) == false)
                        {
                            throw new ApplicationException("The send job completed request returned false!");
                        }
                    }
                    else if (unlockType == MessageTypeTO.JobErrorRequest)
                    {
                        if (client.sendJobErrorRequest(_config, _report) == false)
                        {
                            throw new ApplicationException("The send job error request returned false!");
                        }
                    }
                    markedCompleted = true;
                    break;
                }
                catch (Exception exc)
                {
                    _report.addError("Failed sending the unlock request of type " + Enum.GetName(typeof(MessageTypeTO), unlockType) +
                        " - Will sleep for 1 minute and try again. Send attempts: " + tries, exc);
                    System.Threading.Thread.Sleep(60000); // wait one minute then try again
                }
                finally
                {
                }
            }
        }

        bool checkSiteForWork()
        {
            try
            {
                if (_config == null ||
                    String.IsNullOrEmpty(_config.QueryConfigurations.RootNode.Value.File) ||
                    String.IsNullOrEmpty(_config.SiteCode) ||
                    String.IsNullOrEmpty(_config.QueryConfigurations.RootNode.Value.Fields))
                {
                    throw new ApplicationException("The Vista service has not been properly configured. Unable to continue");
                }

                // get Vista header info for metrics - if we fail, no big deal, we should still proceed
                try
                {
                    // since we're not storing the global, we'll get it dynamically
                    String global = _vistaDao.getDao().getVariableValueQuery(_config.SiteCode, "$G(^DIC(" + _config.QueryConfigurations.RootNode.Value.File + ",0,\"GL\"))");
                    String header = _vistaDao.getDao().getVariableValueQuery(_config.SiteCode, "$G(" + global + "0))");
                    String[] headerPieces = header.Split(new char[] { '^' });
                    _serviceState.LastVistaIEN = headerPieces[2];
                    try
                    {
                        _serviceState.RecordsInFile = Convert.ToInt64(headerPieces[3]);
                    }
                    catch (Exception) { /* swallow */ }
                    _report.addDebug(String.Format("File global: {0} -- File header: {1}", global, header));
                }
                catch (Exception) { /* only affects metrics - not unrecoverable */ }

                if (_config.ExtractMode == ExtractorMode.REBUILD)
                {
                   // _lastSqlIEN = "0"; // this is used for calculating percentage finished so we set it here
                    _vistaQuery.From = _vistaQuery.StartIen = "0"; // set start point to zero if we are rebuilding a file

                }
                else if (_config.ExtractMode == ExtractorMode.INCREMENTAL)
                {
                    //_lastSqlIEN = _config.StartIen; // getting this from Orchestrator!
                    _vistaQuery.From = _vistaQuery.StartIen = _config.StartIen;
                }
                else if (_config.ExtractMode == ExtractorMode.DIFF)
                {
                    if (_config.SqlIens == null || _config.SqlIens.Count == 0)
                    {
                        throw new DownstreamException("DIFF mode was specified but no SQL IENs were provided");
                    }
                    IList<String> iensFromVista = VistaDaoUtils.getIensFromVistaFile(_vistaDao.getDao(), _config.SiteCode, _config.QueryConfigurations.RootNode.Value.File);
                    KeyValuePair<String, String> fromTo = new DataCleaningUtils().getExtractRange(_config.SqlIens, iensFromVista);
                    //_lastSqlIEN = 
                    _config.StartIen = _vistaQuery.From = _vistaQuery.StartIen = fromTo.Key;
                    this.LastIen = fromTo.Value;
                    _report.addInfo(String.Format("Running in diff mode - extracting IENs {0} through {1}", fromTo.Key, fromTo.Value));
                }

                //if (new VistaIenSeeder(null).needsConfigChange(_config.QueryConfigurations.RootNode.Value.File))
                //{
                //    _report.addDebug("This file's configs need updating - start IEN from Orchestartor: " + _config.StartIen);
                //    new VistaIenSeeder(null).updateConfig(_config);
                //    _vistaQuery.Screen = _config.QueryConfigurations.RootNode.Value.Screen; // this is UGLY!!! we're starting to copy properties between objects here and it smells... need to refactor
                //    _vistaQuery.Identifier = _config.QueryConfigurations.RootNode.Value.Identifier; // this is UGLY!!! we're starting to copy properties between objects here and it smells... need to refactor
                //    _lastSqlIEN = _vistaQuery.From = _vistaQuery.StartIen = _config.StartIen = _config.QueryConfigurations.RootNode.Value.From; // set by VistaIenSeeder.updateConfig
                //}
            }
            catch (Exception exc)
            {
                _report.addError("A critical error occurred while trying to verify setup for a Vista service", exc);
                return false; // if don't return true then expect check did not succeed
            }

            return true; // we're not really doing much to check for work so probably ok to just return true if we drop all the way through
        }

        /// <summary>
        /// This method should only be called from a test! It is used to bypass all the server startup, get last IEN, etc. code
        /// </summary>
        /// <param name="report"></param>
        /// <param name="query"></param>
        /// <param name="config"></param>
        /// <param name="fileDao"></param>
        internal void testExecute(ExtractorReport report, VistaQuery query, ExtractorConfiguration config, FileDao fileDao)
        {
            _report = report;
            _vistaQuery = query;
            _config = config;
            _fileDao = fileDao;
            _report.setConfiguration(_config);
            _vistaDao = new VistaDaoImpl();

            // call this to make sure setup is the same
            if (!checkSiteForWork())
            {
                throw new ConfigurationErrorsException("Test execute checkSiteForWork returned false");
            }

            // do this for loop in execute
            _server = new Server(true);
            _server.startListener();

            _serviceState = new VistaServiceState();
            _serviceState.Status = ServiceStatus.RUNNING;

            this.extract();
        }

        //bool lastRowHasZeroIEN(DataTable table)
        //{
        //    if (table == null || table.Rows.Count == 0)
        //    {
        //        return false;
        //    }
        //    return String.Equals((String)table.Rows[table.Rows.Count - 1]["IEN"], "0");
        //}

        //DataTable removeRecordsWithZeroIen(DataTable table)
        //{
        //    DataTable destination = table.Clone();
        //    destination.Rows.Clear();
        //    for (int i = 0; i < table.Rows.Count; i++)
        //    {
        //        if (Convert.ToDecimal(table.Rows[i]["IEN"]) == 0)
        //        {
        //            continue;
        //        }
        //        object[] vals = table.Rows[i].ItemArray;
        //        destination.Rows.Add(vals);
        //    }
        //    return destination;
        //}

        //DataTable removeRecordsAfterLastIen(DataTable table)
        //{
        //    DataTable destination = table.Clone();
        //    destination.Rows.Clear();
        //    Decimal stopPoint = Convert.ToDecimal(LastIen);
        //    for (int i = 0; i < table.Rows.Count; i++)
        //    {
        //        if (Convert.ToDecimal(table.Rows[i]["IEN"]) > stopPoint)
        //        {
        //            break;
        //        }
        //        object[] vals = table.Rows[i].ItemArray;
        //        destination.Rows.Add(vals);
        //    }
        //    return destination;
        //}

        public String LastIen = "";

        void extract()
        {
            _report.addDebug("Successfully verified configuration - starting Vista extraction for: " + _vistaQuery.ToString());

            bool breakExtractLoop = false;
            bool breakExtractLoopAfterNextQuery = false;
            DataTable[] parentResults;
            DataTable results = new DataTable();
            string lastIen = _report.StartIen = _report.LastIen = _vistaQuery.StartIen; // set the start/stop IEN for the report! note: we set the stop report to the start in case we don't receive any records
            string topIen = "0";
            int siteErrorCount = 0;
            int exceptionalsCount = 0;
            //349201
            while (_serviceState.Status != ServiceStatus.STOPPED && !breakExtractLoop) // if the service status has been set to stopped, must have been requested
            {
                if (DateTime.Now.Subtract(_serviceStart).CompareTo(_maxRunTime) > 0)
                {
                    _report.Exceptionals.Add(new Exceptional() 
                    { 
                        Code = ErrorCode.EXCEEDED_MAX_RUN_TIME, 
                        Message = String.Format("Ran for: {0}, Max run time configured: {1}", DateTime.Now.Subtract(_serviceStart).ToString(), _maxRunTime.ToString()) 
                    });
                    _report.addError(String.Format("Ran for: {0}, Max run time configured: {1}", DateTime.Now.Subtract(_serviceStart).ToString(), _maxRunTime.ToString()));
                    _report.Errored = true;
                    breakExtractLoop = true;
                    continue;
                }
                try
                {
                    // Execute our query
                    QueryResults qr = _vistaDao.queryWithDepth(_config, _vistaQuery);
                    if (qr.Exceptionals.Count > 0)
                    {
                        exceptionalsCount++;
                        foreach (Exceptional e in qr.Exceptionals)
                        {
                            _report.Exceptionals.Add(e);
                        }
                    }
                    addSubQueryLogEntries(_vistaDao.getReport()); // get logs from all subfile queries and add to this log
                    parentResults = qr.DdrResults.ToArray();

                    results = parentResults[0];
                    if (results == null)
                    {
                        _report.addDebug("Received a null response from VistaDao.query()");
                        throw new ApplicationException("Vista query failed unexpectedly: " + _vistaQuery.ToString());
                    }
                    if (results.Rows == null || results.Rows.Count == 0)
                    {
                        _report.addDebug("Received an empty table from VistaDao.query(). Looks like we are at the end of the Vista file!");
                        _report.RecordsExtracted = Convert.ToInt32(_serviceState.ParentRecordsExtracted);
                        break; // no more data to be had from Vista for this file in this site!
                    }
                    else
                    {
                        _report.addDebug(String.Format("Received {0} records in the top level file of our query!", results.Rows.Count));
                    }

                    if (breakExtractLoopAfterNextQuery) // this is currently being used only for ticket #120 but may be useful for other files in the future
                    {
                        _report.addDebug("Break after next query flag was set on last iteration so stopping extraction now");
                        breakExtractLoop = true;
                    }

                    // Keep track of and action on the highest ien received in this data set
                    _report.LastIen = _currentExtractorIEN = topIen = getTopIen(results); // Convert.ToDecimal(lastIen);

                    // BCMA special - update 11/14/2013: no longer just BCMA. ExtractorMode.DIFF uses this end param now, too
                    if (!String.IsNullOrEmpty(LastIen) && Convert.ToDecimal(topIen) > Convert.ToDecimal(LastIen))
                    {
                        parentResults[0] = DataCleaningUtils.removeRecordsAfterLastIen(parentResults[0], this.LastIen);
                        breakExtractLoop = true;
                    }
                    // end BCMA
                    _report.addDebug("Last IEN in current top level file from query: " + topIen);

                    qr = QueryResultsUtils.cleanupTicket76(qr, qr.LabChemIens); // strip top level file records per ticket #76
                    qr = QueryResultsUtils.cleanupTicket85(qr); // change "&amp;" to just "&" in lab accession number per ticket #85

                    // ticket #15 - remove rows with zero
                    if (!breakExtractLoop)
                    {
                        breakExtractLoop = QueryResultsUtils.checkTicket15(qr, _config);
                        if (breakExtractLoop)
                        {
                            _report.addInfo("Found 0 IEN in results! This issue was reported in ticket #15 in Assembla. For now, treating this as a complete file traversal and signaling complete");
                        }
                    }
                    // end ticket #15
                    // ticket #73 - only check if break hasn't already been set
                    if (!breakExtractLoop)
                    {
                        breakExtractLoop = QueryResultsUtils.checkTicket73(qr, _config);
                    }
                    // end ticket #73
                    // ticket #81
                    if (!breakExtractLoop)
                    {
                        breakExtractLoop = QueryResultsUtils.checkTicket81(qr, _config);
                        if (breakExtractLoop)// we stripped the last IEN! should get new top IEN
                        {
                            _report.addDebug("Removed record from results - site 556, file 52. Per ticket #81");
                            _report.LastIen = _currentExtractorIEN = topIen = getTopIen(results);
                            _report.addDebug("Last IEN in current top level file from query: " + topIen);
                        }
                    }
                    // end #81


                    if (String.Equals(topIen, lastIen)) // == Convert.ToDecimal(lastIen))
                    {
                        // our result set includes no new data. We're done here
                        _report.addDebug("The last IEN from this query is the same as the last IEN from our last query! No new results - finished extracting");
                        break;
                    }

                    if (DataCleaningUtils.lastRowHasZeroIEN(parentResults[0]))
                    {
                        parentResults[0] = DataCleaningUtils.removeRecordsWithZeroIen(parentResults[0]);
                        breakExtractLoop = true;
                        Report.addInfo("The last IEN in this query was zero - scrubbed the records with IEN of zero and breaking loop since extractor would otherwise enter infinite loop");
                    }


                    foreach (DataTable result in parentResults)
                    {
                        if (result == null || result.Rows.Count == 0)
                        {
                            _report.addDebug("Found a DataTable with zero records! This shouldn't happen - need to fix. Vista file: " + result.TableName + " Query:\r\n" + _vistaQuery.ToString());
                        }
                    }
                    _fileDao.saveFilesWithRollback(parentResults, _config.ExtractMode, 10);
                    // need to check if we're querying lab data with lab chem
                    if (LabChemUtils.containsLabChemConfig(_config) && !String.IsNullOrEmpty(qr.StringResult))
                    {
                        _fileDao.saveToFile(qr.StringResult, _fileDao.DownstreamExtractsDirectory + "LabChem" + topIen + ".downstream");
                    }
                    // end lab chem

                    // metrics!
                    try
                    {
                        _serviceState.CurrentIEN = topIen;
                        _serviceState.ParentRecordsExtracted += results.Rows.Count;
                        for (int i = 1; i < qr.DdrResults.Count; i++)
                        {
                            _serviceState.ChildRecordsExtracted += qr.DdrResults[i].Rows.Count;
                        }
                        _serviceState.PercentageComplete = ((Convert.ToDecimal(_config.StartIen) + _serviceState.ParentRecordsExtracted) / _serviceState.RecordsInFile).ToString(); // add the start IEN to the number of records extracted to try and calculate accurate incremental extraction percentages
                        _report.addDebug("Current metrics: " + _serviceState.ToString());
                    }
                    catch (Exception) { /* no big whoop - just may miss the metrics */ }
                    // end metrics

                    // we want to do this AFTER saving the results so we can look at them later
                    if (!breakExtractLoop && Convert.ToDecimal(topIen) < Convert.ToDecimal(lastIen)) // need to check if special error handling above already caught this...
                    {
                        _report.addInfo("Breaking extraction loop to prevent possible infinite loop...");
                        _report.addError("Looks like the last query's top IEN was less than the last query's top IEN! Marking this job as errored but NOT deleting files. The files should be manually cleaned of the 'dirty' records but are otherwise probably ok (don't forget to check the IEN tracking table!!!)");
                        //_report.Errored = true; // code below deletes files if errored!
                        _report.RecordsExtracted = 0; // set this to zero so we can catch problem
                        _report.Exceptionals.Add(new Exceptional() { Code = ErrorCode.INFINITE_LOOP, Message = "Check extracted data files for out of order IENs" });
                        break;
                    }

                    siteErrorCount = 0; // reset if everything worked out
                }
                catch (DownstreamFileTransactionException)
                {
                    _report.addError("There was an unrecoverable error saving data files as a transaction. This will need to be cleaned manually!");
                    _report.Exceptionals.Add(new Exceptional() { 
                        Code = ErrorCode.EXCEEDED_MAX_RUN_TIME, 
                        Message = "There was an unrecoverable error saving data files as a transaction. This will need to be cleaned manually!" });
                    _report.Errored = true;
                    breakExtractLoop = true;
                }
                catch (NoDataInSiteAndFileException)
                {
                    _report.addInfo("There appears to be no data in this site/file - breaking extract loop");
                    breakExtractLoop = true;
                }
                catch (Exception ex)
                {
                    _report.addError("Generic error in extract loop", ex);
                    // ticket 120
                    // there is an issue with the end of file 200 in Indy, use quick binary search algo to discover how many records are safe to pull
                    if (ex.Message.Contains("M  ERROR") && String.Equals(_config.QueryConfigurations.RootNode.Value.File, "200") && String.Equals(_config.SiteCode, "583"))
                    {
                        _vistaQuery.MaxRecords = new BinarySearchUtil(_vistaDao.getDao()).getMaxRexForLastQuery(_vistaQuery.From, _vistaQuery.MaxRecords);
                        breakExtractLoopAfterNextQuery = true;
                        _report.addDebug("Ticket #120 - using binary search, found max records for next query: " + _vistaQuery.MaxRecords + ". Breaking after next query");
                        continue;
                    }

                    // ticket 59
                    // file 356 & 45 consistently times out on the very last query - don't know what the deal is but it's not really an error and we don't want to log it that way
                    if (String.Equals(_config.QueryConfigurations.RootNode.Value.File, "356") || String.Equals(_config.QueryConfigurations.RootNode.Value.File, "45"))
                    {
                        breakExtractLoop = true;
                        continue; // while loop should key off the breakExtractLoop boolean
                    }
                    //end ticket 59
                    // let's give each site a chance to have a couple errors since it shouldn't affect stability of anything
                    siteErrorCount++;
                    if (siteErrorCount < 3)
                    {
                        _report.addDebug("An error has been generated while executing a Vista query " + siteErrorCount +
                            " times. Will try " + (3 - siteErrorCount) + " more times. Query: " + _vistaQuery);
                        Thread.Sleep(1000 * 60);
                        continue;
                    }
                    try
                    {
                        _report.addDebug("This extraction job failed unacceptable number of times! Deleting all extracted files and exiting");
                        _fileDao.deleteFiles(_fileDao.SavedFiles);
                        _fileDao.SavedFiles = new List<FileInfo>();
                    }
                    catch (Exception)
                    {
                        _report.addError("An error occurred while trying to clean up saved files on unrecoverable VistaService failure");
                    }
                    throw;
                }
                ////this is here to help with tests
                //_server.stopListener(); // stop the server listener so loop will cease

                // set up query for next go
                //if (results.Rows != null && results.Rows.Count > 0) // it's not clear why we're checking the results here - was causing an issue reported in ticket #101 so just removing for now. i don't think this should be an issue since we would break if received zero records
                //{
                string topIenStr = topIen.ToString();
                _vistaQuery.StartIen = topIenStr;
                _vistaQuery.From = topIenStr;
                _currentExtractorIEN = topIenStr;
                lastIen = topIenStr;
                //}

                // If you are using a cross-reference, you only get one shot at it since the MDO api doesn't return the looping reference
                // and IEN is no longer the primary index
                if (!_vistaQuery.XREF.Equals("#"))
                {
                    _report.addInfo("Quitting extraction loop due to non IEN cross reference");
                    break;
                }


                // put beneath everything here so we don't screw up the metrics!!!
                if (breakExtractLoop) // break out of loop AFTER saving results - helper functions should clean up datatable
                {
                    break;
                }

                // Force a GC
                GC.Collect();
            }

            _report.RecordsExtracted = Convert.ToInt32(_serviceState.ParentRecordsExtracted);

            // Force a GC
            GC.Collect();

            if (_serviceState.Status == ServiceStatus.STOPPED)
            {
                _report.RecordsExtracted = 0;
                _report.LastIen = "0";
                _report.addDebug("It appears someone asked us to shutdown! So, we're deleting all our extracts");
                _report.Exceptionals.Add(new Exceptional() { Code = ErrorCode.MANUAL_SHUTDOWN, Message = "This extraction process was manually stopped" });
                _report.Errored = true; // we want to mark these as errored even though technically nothing terrible happened - we probably need to process manually if we manually intervened
                //_fileDao.deleteFiles(_fileDao.SavedFiles);
                //_fileDao.SavedFiles = new List<FileInfo>();
            }
            if (_serviceState.Status == ServiceStatus.STOPPED || _report.Errored)
            {
                try
                {
                    _report.RecordsExtracted = 0;
                    _report.LastIen = "0";
                    _fileDao.deleteFiles(_fileDao.SavedFiles);
                    _fileDao.SavedFiles = new List<FileInfo>();
                }
                catch (Exception exc)
                {
                    _report.addError("Unable to cleanup the saved files even though report was marked as errored or stopped!", exc);
                }
            }
        }

        internal void addSubQueryLogEntries(domain.reporting.Report report)
        {
            if (report != null && _logSubqueryMessages)
            {
                _report.addInfo(" == Begin subfile query messages ==");

                _report.addInfo("Query info msgs:");
                foreach (String infoMsg in report.InfoMessages)
                {
                    _report.addInfo(infoMsg);
                }

                _report.addInfo("Query debug msgs:");
                foreach (String infoMsg in report.DebugMessages)
                {
                    _report.addInfo(infoMsg);
                }

                _report.addInfo("Query error msgs:");
                foreach (String infoMsg in report.ErrorMessages)
                {
                    _report.addInfo(infoMsg);
                }

                _report.addInfo(" == End subfile query messages ==");

                report.clear(); // be sure to clear the DAO impl's report and not our own!
            }
        }

        public override ServiceState getServiceState()
        {
            if (_serviceState == null)
            {
                _serviceState = new VistaServiceState();
            }
            return _serviceState;
        }

        public override ServiceState setServiceState(ServiceStatus setTo)
        {
            getServiceState();
            _serviceState.Status = setTo;
            return _serviceState;
        }

        /// <summary>
        /// Examine all the iens in the table as integers and return the greatest value
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        public String getTopIen(DataTable table)
        {
            String rowIenStr = "0";
            String topIenStr = "0";
            Decimal rowIen = 0;
            Decimal topIen = 0;
            foreach (DataRow row in table.Rows)
            {
                rowIenStr = (String)row["IEN"];
                rowIen = Convert.ToDecimal(rowIenStr);
                if (rowIen > topIen)
                {
                    topIen = rowIen;
                    topIenStr = rowIenStr;
                }
            }

            return topIenStr;
        }
    }
}
