using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using com.bitscopic.downstream.dao.file;
using com.bitscopic.downstream.dao.sql;
using com.bitscopic.downstream.dao.vista;
using com.bitscopic.downstream.domain;
using com.bitscopic.downstream.domain.data;
using com.bitscopic.downstream.net;
using com.bitscopic.downstream.service;
using System.Configuration;
using System.Threading;
using System.Net.Mail;
using com.bitscopic.downstream.domain.exception;
using System.Reflection;
using System.Net;
using com.bitscopic.downstream.domain.reporting;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Data;
using gov.va.medora.mdo;
using gov.va.medora.mdo.dao.ldap;
using System.IO;

namespace com.bitscopic.downstream
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application. Starts a type of service
        /// </summary>
        static void Main(string[] args)
        {
            // goofy bit of code to enable using this executable for CDW BCMA data extractions
            //if (!String.IsNullOrEmpty(ConfigurationManager.AppSettings["ServiceType"]) && String.Equals("CdwBcmaService", ConfigurationManager.AppSettings["ServiceType"], StringComparison.CurrentCultureIgnoreCase))
            //{
            //    logging.Log.LOG("Looks like you're using the super cool CDW BCMA extractor! Good job!");
            //    if (args == null || args.Length == 0)
            //    {
            //        args = new String[] { ConfigurationManager.AppSettings["BcmaSite"] };
            //    }
            //    String currentSite = "";
            //    if (args[0].Contains(";"))
            //    {
            //        String[] siteList = args[0].Split(new char[] { ';' });
            //        for (int i = 0; i < siteList.Length; i++)
            //        {
            //            try
            //            {
            //                currentSite = siteList[i];
            //                extractBcmaFromCdw(new String[] { currentSite });
            //                moveBcmaFiles(currentSite);
            //                GC.Collect();
            //            }
            //            catch (Exception) { /* just continue churning through */ }
            //        }
            //    }
            //    else
            //    {
            //        currentSite = args[0];
            //        extractBcmaFromCdw(args);
            //        moveBcmaFiles(currentSite);
            //    }
            //    Environment.Exit(0);
            //    return;
            //}

            logging.Log.LOG("Starting up!");
            new VistaDaoFactory().getVistaDao(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.VistaDaoType]);
            System.Threading.Thread.Sleep(5000); // let connection pool start if using MdoVistaDao - these two lines are a stopgap measure - need to fix ConnectionPool in MDO

            Int32 processId = Process.GetCurrentProcess().Id;

            Int32 workerThreads = 8;
            Int32 delayBetweenStart = 1000;

            // start mgt server for controlling starting/stopping workers
            Server mgtServer = startMgtServer();

            if (!String.IsNullOrEmpty(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.WorkerThreads]))
            {
                Int32.TryParse(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.WorkerThreads], out workerThreads); // try to set this - if it fails then we use default
            }

            IList<Task> allTasks = new List<Task>();

            for (int i = 0; i < workerThreads; i++)
            {
                Task workerTask = new Task(() => runService(String.Concat(processId.ToString(), "_", i.ToString())));
                allTasks.Add(workerTask);
                workerTask.Start();

                System.Threading.Thread.Sleep(delayBetweenStart);
            }

            // this is a simple thread pool of sorts - just periodically check if a thread is still running and re-use the slot if not
            TimeSpan sleepTime = new TimeSpan(0, 0, 5);
            if (!String.IsNullOrEmpty(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.ExtractorSleepTime])) // need to check that this isn't empty because otherwise TimeSpan.TryParse will evaulate to zero and loop will cause 100% CPU utilization
            {
                TimeSpan.TryParse(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.ExtractorSleepTime], out sleepTime);
            }

            // main loop - runs forever, sleeping for configurable time, checking for work and starting new jobs if needed
            while (true)
            {
                if (mgtServer.SocketContainer.Locked)
                {
                    System.Threading.Thread.Sleep(new TimeSpan(0, 1, 0));
                    logging.Log.LOG("Server is currently locked - not starting new jobs");
                    continue;
                }

                for (int i = 0; i < workerThreads; i++) // want to check if task in a slot is complete every time we wake - we'll worry about assigning a new task to that slot below
                {
                    if (allTasks[i] != null && allTasks[i].Status == TaskStatus.RanToCompletion)
                    {
                        allTasks[i] = null;
                        GC.Collect();
                    }
                }

                if (checkForWork()) // before we potentially spin up a bunch of threads to all ask the same thing, we should make a smaller request to see if there is work that needs to be done
                {

                    for (int i = 0; i < workerThreads; i++)
                    {
                        if (allTasks[i] == null) // if slot has been nulled
                        {
                            allTasks[i] = new Task(() => runService(String.Concat(processId.ToString(), "_", i.ToString())));
                            allTasks[i].Start();
                        }
                    }
                }
                else // if server says no work, we should sleep for extra time since asking again in just a few seconds probably is unneccessary
                {
                    System.Threading.Thread.Sleep(new TimeSpan(0, 5, 0));
                }

                System.Threading.Thread.Sleep(sleepTime);
            }
        }

        private static Server startMgtServer()
        {
            Server mgtServer = new Server();
            mgtServer.SocketContainer.ListeningPort = Convert.ToInt32( ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.ExtractorMgrListeningPort]);
            mgtServer.startListener();
            return mgtServer;
        }

        static void moveBcmaFiles(String siteId)
        {
            // lets' move the files in to a directory named for the site ID
            String siteDirPath = String.Concat(@"C:\Downstream\", siteId);
            if (!Directory.Exists(siteDirPath))
            {
                Directory.CreateDirectory(siteDirPath);
            }
            FileInfo[] newFiles = new DirectoryInfo(@"C:\Downstream\").GetFiles(siteId + "_*");
            foreach (FileInfo fi in newFiles)
            {
                File.Move(fi.FullName, String.Concat(siteDirPath, "\\", fi.Name));
            }

            // end move files
        }

        static ExtractorConfiguration getExtractorConfigFromDb(String vistaFile)
        {
            ISqlDao sqlDao = new SqlDaoFactory().getSqlDao(new SqlConnectionFactory().getConnection(
                ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SqlProvider],
                ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SqlConnectionString]));
            IList<ExtractorConfiguration> allConfigs = sqlDao.getActiveExtractorConfigurations();

            ExtractorConfiguration selected = null;
            foreach (ExtractorConfiguration ec in allConfigs)
            {
                if (String.Equals(ec.QueryConfigurations.RootNode.Value.File, vistaFile))
                {
                    selected = ec;
                    break;
                }
            }
            return selected;
        }


        static void runService(object processId)
        {
            AbstractService svc = null;
            
            try
            {
                svc = new VistaService(); // TODO  refactor further to detect additional extractor service types at runtime
                svc.Name = Convert.ToString(processId);
                svc.execute();
            }
            catch (Exception) { }
            finally
            {
                if (((VistaService)svc).Report != null)
                {
                    bool createLocalRpt = false;
                    Boolean.TryParse(ConfigurationManager.AppSettings["CreateLocalTextReport"], out createLocalRpt);

                    if (createLocalRpt)
                    {
                        try
                        {
                            new FileDao().saveToFile(((VistaService)svc).Report.ToString(), "C:\\Downstream\\ExtractorReport.txt");
                        }
                        catch (Exception exc)
                        {
                            //System.Console.WriteLine(exc.ToString());
                        }
                    }
                }
            }
        }

        static bool checkForWork()
        {
            try
            {
                Client c = new Client(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.OrchestratorHostName], Convert.ToInt32(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.OrchestratorListeningPort]));
                return c.sendServerHasWorkRequest();
            }
            catch (Exception) // if this fails, assume server does have work
            {
                return true;
            }
        }


        static void extractBcmaFromCdw(string[] args)
        {
            String sitecode = args[0];
            KeyValuePair<String, String> range = getRangeFromCdw(sitecode);
            //String startIenFirstWorker = args[1];
            //String lastIenFromCdw = getGreatestCdwIen(sitecode);
            logging.Log.LOG(String.Format("IEN range from CDW for {0} : {1} to {2}", sitecode, range.Key, range.Value));

            Int32 workerThreads = 8;

            IList<String> startPoints = getStartPointsForWorkerThreads(range.Key, range.Value, workerThreads);

            ExtractorConfiguration config = getExtractorConfig("53.79");
            config.QueryConfigurations.RootNode.Value.IdentifiedFiles = config.QueryConfigurations.RootNode.Value.Identifier = "";
            config.QueryConfigurations.RootNode.Children = new List<TreeNode<QueryConfiguration>>();
            config.SiteCode = config.Sites = sitecode;

            logging.Log.LOG(String.Format("Getting ready to use {0} workers to extract top level file from Vista", workerThreads.ToString()));

            IList<Task> allTasks = new List<Task>();
            for (int i = 0; i < startPoints.Count - 1; i++)
            {
                String start = startPoints[i];
                String end = startPoints[i + 1];
                Task vs = new Task(() => extractWithStopIen(config, start, end));
                allTasks.Add(vs);
                vs.Start();
            }

            Task lastChunk = new Task(() => extractWithStopIen(config, startPoints[startPoints.Count - 1], range.Value));
            allTasks.Add(lastChunk);
            lastChunk.Start();

            foreach (Task t in allTasks)
            {
                t.Wait();
            }

            logging.Log.LOG("Finished extracting top level file from Vista!! Only CDW subfiles left...");

            MdoVistaDao.getInstance().shutdownPool(); // don't need our vista cxns any more - let's be nice and shutdown our connections

            try
            {
                createBcmaSubFileTables(sitecode, range.Key, range.Value);
            }
            catch (Exception exc)
            {
                logging.Log.LOG(exc.ToString());
            }

            try
            {
                logging.Log.LOG("Getting ready to update IEN tracking table...");
                ISqlDao dao = new SqlDaoFactory().getSqlDao(new SqlConnectionFactory().getConnection(ConfigurationManager.AppSettings["SqlProvider"], ConfigurationManager.AppSettings["SqlConnectionString"]));
                dao.saveLastIen(sitecode, "53.79", range.Key, range.Value, "20010101125959");
                logging.Log.LOG("Successfully updated IEN tracking table!");
            }
            catch (Exception exc)
            {
                logging.Log.LOG(String.Format("Oh geez... there was a problem updating the IEN tracking table with start IEN {0} and stop IEN {1} for site {2}", range.Key, range.Value, sitecode) + " - " + exc.Message);
            }

            logging.Log.LOG("Finished with hybrid BCMA extraction. Wicked cool.");

            return;
        }

        static void extractWithStopIen(ExtractorConfiguration config, String startIen, String stopIen)
        {
            VistaQuery query = new VistaQuery(config, config.QueryConfigurations.RootNode.Value);
            query.StartIen = query.From = startIen;
            query.SiteCode = config.SiteCode;

            VistaService svc = new VistaService();
            svc.LastIen = stopIen;
            svc.testExecute(new domain.reporting.ExtractorReport("TestID"), query, config, new FileDao(false));
        }

        static IList<String> getStartPointsForWorkerThreads(String startIen, String lastCdwIen, Int32 workers)
        {
            IList<String> result = new List<String>();
            result.Add(startIen);

            Int32 startIenInt = Convert.ToInt32(startIen);
            Int32 lastCdwIenInt = Convert.ToInt32(lastCdwIen);

            Int32 difference = lastCdwIenInt - startIenInt;
            Int32 workerChunk = difference / workers; // number of workers


            for (int i = 0; i < workers - 1; i++)
            {
                result.Add((Convert.ToInt32(result[i]) + workerChunk).ToString());
            }
            return result;
        }


        static KeyValuePair<String, String> getRangeFromCdw(String sitecode)
        {
            String sql = "SELECT MIN(CAST(BCMAMedicationLogIEN AS INTEGER))-1 FROM BCMA.BCMAMedicationLog WHERE  Sta3n=" + sitecode + " AND EnteredVistaDate LIKE('3100101%') UNION " +
                "SELECT MAX(CAST(BCMAMedicationLogIEN AS INTEGER)) FROM BCMA.BCMAMedicationLog WHERE Sta3n=" + sitecode;
            //String sql = "SELECT MAX(CAST(BCMAMedicationLogIEN AS INTEGER)) FROM BCMA.BCMAMedicationLog WHERE Sta3n=" + sitecode;
            gov.va.medora.mdo.User impersonationUser = new User()
            {
                UserName = ConfigurationManager.AppSettings["CdwUserName"],
                Pwd = ConfigurationManager.AppSettings["CdwUserPassword"],
                Domain = ConfigurationManager.AppSettings["CdwUserDomain"]
            };

            String connectionString = "Data Source=127.0.0.1;Initial Catalog=CDWWork;Trusted_Connection=true";

            using (gov.va.medora.mdo.dao.ldap.Impersonator imp = new Impersonator(impersonationUser))
            {
                using (System.Data.SqlClient.SqlConnection newCxn = new System.Data.SqlClient.SqlConnection(connectionString))
                {
                    newCxn.Open();
                    System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                    cmd.Connection = newCxn;
                    cmd.CommandText = sql;
                    cmd.CommandTimeout = 60 * 60 * 10;
                    IDataReader rdr = cmd.ExecuteReader();
                    rdr.Read();
                    String startIen = Convert.ToString(rdr.GetInt32(0));
                    rdr.Read();
                    String stopIen = Convert.ToString(rdr.GetInt32(0));
                    return new KeyValuePair<string, string>(startIen, stopIen); // Convert.ToString((Int32)cmd.ExecuteScalar());
                }
            }

        }

        static ExtractorConfiguration getExtractorConfig(String vistaFile)
        {
            return new ExtractorConfiguration()
            {
                ExtractMode = service.ExtractorMode.REBUILD,
                //SiteCode = "901",
                //StartIen = "0",
                MaxRecordsPerQuery = "5000",
                QueryConfigurations = new domain.data.Tree<QueryConfiguration>(new domain.data.TreeNode<QueryConfiguration>(new QueryConfiguration()
                {
                    Fields = ".01;.02;.03;.06;.07;.08;.09;.11;.12;.13;.14;.15",
                    File = "53.79",
                    From = "0",
                    FullyQualifiedFile = "/53.79",
                    XREF = "#"
                }))
            };

        }

        static void createBcmaSubFileTables(String sitecode, String startIen, String stopIen)
        {

            String sql = "SELECT LOG.BCMAMedicationLogIEN AS IEN, " +
                "BCMADRUG.BCMADispensedDrugIEN AS \"53.795_IEN\",DRUG.LocalDrugIEN AS \"53.795_.01\", " +
                "BCMADRUG.DosesOrdered AS \"53.795_.02\",BCMADRUG.DosesGiven AS \"53.795_.03\",BCMADRUG.UnitOfAdministration AS \"53.795_.04\",BCMAADD.BCMAAdditiveIEN AS \"53.796_IEN\",ADDTVE.IVAdditiveIngredientIEN AS \"53.796_.01\",BCMAADD.DoseOrdered AS \"53.796_.02\", " +
                "BCMAADD.DoseGiven AS \"53.796_.03\",BCMAADD.UnitOfAdministration AS \"53.796_.04\" " +
                 "FROM BCMA.BCMAMedicationLog LOG " +
                 "LEFT JOIN BCMA.BCMADispensedDrug BCMADRUG ON BCMADRUG.BCMAMedicationLogSID=LOG.BCMAMedicationLogSID " +
                 "  LEFT JOIN Dim.LocalDrug DRUG ON BCMADRUG.LocalDrugSID=DRUG.LocalDrugSID " +
                 "LEFT JOIN BCMA.BCMAAdditive BCMAADD ON BCMAADD.BCMAMedicationLogSID=LOG.BCMAMedicationLogSID " +
                 "  LEFT JOIN Dim.IVAdditiveIngredient ADDTVE ON BCMAADD.IVAdditiveIngredientSID=ADDTVE.IVAdditiveIngredientSID " +
                 "JOIN SPatient.SPatient PAT ON PAT.PatientSID=LOG.PatientSID " +
                 "JOIN Dim.Institution INST ON INST.InstitutionSID=LOG.InstitutionSID " +
                 "JOIN Staff.Staff STAFF  ON LOG.ActionByStaffSID=STAFF.StaffSID " +
                 "JOIN Dim.PharmacyOrderableItem PHARM ON PHARM.PharmacyOrderableItemSID=LOG.PharmacyOrderableItemSID " +
                " WHERE LOG.Sta3n=" + sitecode + " AND CAST(LOG.BCMAMedicationLogIEN AS INTEGER)>" + startIen +
                " AND CAST(LOG.BCMAMedicationLogIEN AS INTEGER)<=" + stopIen + " ORDER BY LOG.BCMAMedicationLogIEN ASC;";


            gov.va.medora.mdo.dao.mock.MockDataReader rdr = query(sql);

            String retrievalTime = DateTime.Now.ToString();
            Dictionary<String, IList<object[]>> table53x795ValsByPien = new Dictionary<string, IList<object[]>>();
            Dictionary<String, IList<object[]>> table53x796ValsByPien = new Dictionary<string, IList<object[]>>();

            while (rdr.Read())
            {
                String parentIen = rdr.GetString(rdr.GetOrdinal("IEN"));

                String ien53x795 = rdr.GetString(rdr.GetOrdinal("53.795_IEN"));
                String x01_53x795 = rdr.GetString(rdr.GetOrdinal("53.795_.01"));
                String x02_53x795 = rdr.IsDBNull(rdr.GetOrdinal("53.795_.02")) ? "" : toVistaNumberFormat(Convert.ToDouble(rdr.GetDecimal(rdr.GetOrdinal("53.795_.02")))); //.ToString();
                String x03_53x795 = rdr.IsDBNull(rdr.GetOrdinal("53.795_.03")) ? "" : toVistaNumberFormat(Convert.ToDouble(rdr.GetDecimal(rdr.GetOrdinal("53.795_.03")))); //.ToString();
                String x04_53x795 = rdr.GetString(rdr.GetOrdinal("53.795_.04"));

                String ien53x796 = rdr.GetString(rdr.GetOrdinal("53.796_IEN"));
                String x01_53x796 = rdr.GetString(rdr.GetOrdinal("53.796_.01"));
                String x02_53x796 = rdr.GetString(rdr.GetOrdinal("53.796_.02"));
                String x03_53x796 = rdr.GetString(rdr.GetOrdinal("53.796_.03"));
                String x04_53x796 = rdr.GetString(rdr.GetOrdinal("53.796_.04"));

                if (!String.IsNullOrEmpty(ien53x795))
                {
                    if (!table53x795ValsByPien.ContainsKey(ien53x795 + "_" + parentIen))
                    {
                        table53x795ValsByPien.Add(ien53x795 + "_" + parentIen, new List<object[]>());
                    }
                    table53x795ValsByPien[ien53x795 + "_" + parentIen].Add(new object[] { parentIen, (ien53x795 + "_" + parentIen), sitecode, retrievalTime, x01_53x795, x02_53x795, x03_53x795, x04_53x795 });
                }

                if (!String.IsNullOrEmpty(ien53x796))
                {
                    if (!table53x796ValsByPien.ContainsKey(ien53x796 + "_" + parentIen))
                    {
                        table53x796ValsByPien.Add(ien53x796 + "_" + parentIen, new List<object[]>());
                    }
                    table53x796ValsByPien[ien53x796 + "_" + parentIen].Add(new object[] { parentIen, (ien53x796 + "_" + parentIen), sitecode, retrievalTime, x01_53x796, x02_53x796, x03_53x796, x04_53x796 });
                }

                // incremental saves to avoid big files that cause out of memory
                if (table53x795ValsByPien.Count > 100000)
                {
                    buildDataTablesAndSave(table53x795ValsByPien, table53x796ValsByPien);
                    table53x795ValsByPien.Clear();
                    table53x796ValsByPien.Clear();

                    GC.Collect();
                }
            }

            // add last batch
            if (table53x795ValsByPien.Count > 0 || table53x796ValsByPien.Count > 0)
            {
                buildDataTablesAndSave(table53x795ValsByPien, table53x796ValsByPien);
            }

        }

        static void buildDataTablesAndSave(Dictionary<String, IList<object[]>> table53x795ValsByPien, Dictionary<String, IList<object[]>> table53x796ValsByPien)
        {
            DataTable dt53x795 = new DataTable("53.795");
            dt53x795.Columns.Add("P_IEN");
            dt53x795.Columns.Add("IEN");
            dt53x795.Columns.Add("SiteCode");
            dt53x795.Columns.Add("RetrievalTime");
            dt53x795.Columns.Add(".01");
            dt53x795.Columns.Add(".02");
            dt53x795.Columns.Add(".03");
            dt53x795.Columns.Add(".04");
            foreach (String key in table53x795ValsByPien.Keys)
            {
                foreach (Object[] vals in table53x795ValsByPien[key])
                {
                    dt53x795.Rows.Add(vals);
                }
            }

            DataTable dt53x796 = new DataTable("53.796");
            dt53x796.Columns.Add("P_IEN");
            dt53x796.Columns.Add("IEN");
            dt53x796.Columns.Add("SiteCode");
            dt53x796.Columns.Add("RetrievalTime");
            dt53x796.Columns.Add(".01");
            dt53x796.Columns.Add(".02");
            dt53x796.Columns.Add(".03");
            dt53x796.Columns.Add(".04");
            foreach (String key in table53x796ValsByPien.Keys)
            {
                foreach (Object[] vals in table53x796ValsByPien[key])
                {
                    dt53x796.Rows.Add(vals);
                }
            }

            new FileDao(false).saveToFile(dt53x795, service.ExtractorMode.REBUILD);
            new FileDao(false).saveToFile(dt53x796, service.ExtractorMode.REBUILD);
        }

        static string toVistaNumberFormat(Double number)
        {
            if (number == 0)
            {
                return "0";
            }
            else if (number < 0)
            {
                return String.Concat("-", Math.Abs(number).ToString().TrimStart('0'));
            }
            else
            {
                return number.ToString().TrimStart('0');
            }
        }


        static gov.va.medora.mdo.dao.mock.MockDataReader query(string request)
        {
            try
            {
                gov.va.medora.mdo.User impersonationUser = new User()
                {
                    UserName = ConfigurationManager.AppSettings["CdwUserName"],
                    Pwd = ConfigurationManager.AppSettings["CdwUserPassword"],
                    Domain = ConfigurationManager.AppSettings["CdwUserDomain"]
                };

                using (new Impersonator(impersonationUser))
                {
                    using (System.Data.SqlClient.SqlConnection newCxn = new System.Data.SqlClient.SqlConnection("Data Source=127.0.0.1;Initial Catalog=CDWWork;Trusted_Connection=true"))
                    {
                        newCxn.Open();
                        System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
                        cmd.Connection = newCxn;
                        cmd.CommandText = request;
                        cmd.CommandTimeout = 60 * 60 * 10;
                        System.Data.SqlClient.SqlDataReader rdr = cmd.ExecuteReader();
                        // the SqlDataReader will be closed at the exit of this using block so we copy everything over to our MockDataReader where it will be cached in a DataTable
                        gov.va.medora.mdo.dao.mock.MockDataReader mock = new gov.va.medora.mdo.dao.mock.MockDataReader();
                        DataTable newTable = new DataTable();
                        newTable.Load(rdr);
                        mock.Table = newTable; // the previous couple lines are broken out so the setter on MockDataReader.Table can properly map the column names - IMPORTANT!!
                        return mock;
                    }
                }

            }
            catch (Exception)
            {
                throw;
            }
        }

    }
}
