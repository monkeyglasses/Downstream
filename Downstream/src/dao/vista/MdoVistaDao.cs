using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using gov.va.medora.mdo.dao;
using gov.va.medora.mdo.dao.vista;
using gov.va.medora.mdo;
using System.Collections.Concurrent;
using System.Threading;
using gov.va.medora.mdo.domain.pool.connection;
using gov.va.medora.mdo.domain.pool;
using System.Configuration;
using com.bitscopic.downstream.domain;
using System.Data;
using com.bitscopic.downstream.utils;

namespace com.bitscopic.downstream.dao.vista
{
    public class MdoVistaDao : IVistaDao
    {
        AbstractConnection _cxn;
        AbstractCredentials _downstreamCreds;
        SiteTable _extractorSites;

        #region Singleton
        static readonly object _locker = new object();
        private static MdoVistaDao _singleton;
        public static MdoVistaDao getInstance()
        {
            if (_singleton == null)
            {
                lock (_locker)
                {
                    if (_singleton == null)
                    {
                        _singleton = new MdoVistaDao();
                    }
                }
            }
            return _singleton;
        }

        private MdoVistaDao()
        {
            initializePool();
        }

        internal void shutdownPool()
        {
            try
            {
                ConnectionPools.getInstance().shutdown();
            }
            catch (Exception) { }
        }

        internal void initializePool()
        {
            logging.Log.LOG("Initializing connection pools...");
            if (ConnectionPools.getInstance().PoolSource != null)
            {
                logging.Log.LOG("Connection pools already initialized");
                return; // already set up the pools!
            }

            SiteTable sites = new SiteTable(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.VhaSitesFilePath]);
            IList<AbstractPoolSource> sources = new List<AbstractPoolSource>();
            ConnectionPoolsSource poolsSource = new ConnectionPoolsSource();
            poolsSource.CxnSources = new Dictionary<string, ConnectionPoolSource>();
            VistaDao trash = new VistaDao();
            AbstractCredentials creds = getDownstreamCredentialsFromConfig(getDownstreamUserFromConfig());

            foreach (DataSource source in sites.Sources)
            {
                if (!String.Equals(source.Protocol, "VISTA", StringComparison.CurrentCultureIgnoreCase))
                {
                    continue;
                }
                ConnectionPoolSource newSource = new ConnectionPoolSource()
                {
                    Timeout = TimeSpan.Parse(ConfigurationManager.AppSettings["PoolConnectionTimeout"]),
                    WaitTime = TimeSpan.Parse(ConfigurationManager.AppSettings["PoolWaitTimeout"]),
                    MaxPoolSize = Convert.ToInt32(ConfigurationManager.AppSettings["PoolMaxSize"]),
                    MinPoolSize = Convert.ToInt32(ConfigurationManager.AppSettings["PoolMinSize"]),
                    PoolExpansionSize = Convert.ToInt32(ConfigurationManager.AppSettings["PoolExpansionSize"]),
                    CxnSource = source,
                    Credentials = creds,
                    Permission = new MenuOption(ConfigurationManager.AppSettings["PoolUserPermission"])
                };
                Int32 recycleCount = 0;
                if (!String.IsNullOrEmpty(ConfigurationManager.AppSettings["PoolRecycleCount"]) && Int32.TryParse(ConfigurationManager.AppSettings["PoolRecycleCount"], out recycleCount))
                {
                    newSource.RecycleCount = recycleCount;
                }
                newSource.CxnSource.Protocol = "PVISTA";
                poolsSource.CxnSources.Add(source.SiteId.Id, newSource);
            }

            ConnectionPools pools = (ConnectionPools)AbstractResourcePoolFactory.getResourcePool(poolsSource);
            logging.Log.LOG("Successfully completed connection pools initialization");
        }

        internal AbstractCredentials getDownstreamCredentialsFromConfig(User user)
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

        internal User getDownstreamUserFromConfig()
        {
            User user = new User();
            Site userSite = new Site(ConfigurationManager.AppSettings["PoolUserHomeSiteCode"], ConfigurationManager.AppSettings["PoolUserHomeSiteName"]);
            user.LogonSiteId = new SiteId(userSite.Id, userSite.Name);
            user.Name = new PersonName(ConfigurationManager.AppSettings["PoolUserName"]);
            user.Uid = ConfigurationManager.AppSettings["PoolUserID"];
            user.SSN = new SocSecNum(ConfigurationManager.AppSettings["PoolUserFedUID"]);
            user.PermissionString = ConfigurationManager.AppSettings["PoolUserPermission"];
            if (!String.IsNullOrEmpty(ConfigurationManager.AppSettings["PoolUserUsername"]) && !String.IsNullOrEmpty(ConfigurationManager.AppSettings["PoolUserPassword"]))
            {
                user.UserName = ConfigurationManager.AppSettings["PoolUserUsername"];
                user.Pwd = ConfigurationManager.AppSettings["PoolUserPassword"];
            }
            return user;
        }

        #endregion

        public string[] ddrLister(string siteId, string vistaFile, string iens, string fields, string flags, string maxRex, string from, string part, string xref, string screen, string identifier)
        {
            AbstractConnection cxn = null;

            try
            {
                logging.Log.logDDR(String.Format("DDR LISTER: Site {0}, File {1}, IENS {2}, Fields {3}, Flags {7}, From {4}, Screen {5}, Identifier {6}", siteId, vistaFile, iens, fields, from, screen, identifier, flags));
                cxn = (AbstractConnection)ConnectionPools.getInstance().checkOutAlive(siteId);

                DdrLister ddr = new DdrLister(cxn);
                ddr.File = vistaFile;
                ddr.Iens = iens;
                ddr.Fields = fields;
                ddr.Flags = flags;
                if (String.Equals(flags, "I") && !String.IsNullOrEmpty(identifier))
                {
                    ddr.Options = "WID"; // this is how we get identifier part for unpacked query results
                }
                if (String.IsNullOrEmpty(xref))
                {
                    ddr.Xref = "#";
                }
                else
                {
                    ddr.Xref = xref;
                }
                ddr.Max = maxRex;
                ddr.From = from;
                ddr.Part = part;
                ddr.Screen = screen;
                ddr.Id = identifier;

                String[] result = null;
                result = ddr.execute();
                return result;
            }
            catch (UnauthorizedAccessException uae)
            {
                logging.Log.LOG("Connection not properly authenticated: " + uae.Message);
                cxn.disconnect(); // will cause cxn pool to discard
                throw uae;
            }
            catch (System.Net.Sockets.SocketException se)
            {
                logging.Log.LOG("Socket error: " + se.Message);
                cxn.disconnect(); // will cause cxn pool to discard
                throw se;
            }
            catch (gov.va.medora.mdo.exceptions.ConnectionException ce)
            {
                logging.Log.LOG("Cxn error: " + ce.Message);
                cxn.disconnect(); // will cause cxn pool to discard
                throw ce;
            }
            catch (Exception exc)
            {
                logging.Log.LOG("DDR LISTER Exception: " + exc.Message);
                throw exc;
            }
            finally
            {
                try
                {
                    ConnectionPools.getInstance().checkIn(cxn);
                }
                catch (Exception) { }
            }

        }

        public string[] ddrGetsEntry(string siteId, string vistaFile, string iens, string flds, string flags)
        {
            AbstractConnection cxn = null;

            try
            {
                logging.Log.LOG(String.Format("DDR GETS ENTRY: {0}, {1}, {2}, {3}", siteId, vistaFile, iens, flds));
                cxn = (AbstractConnection)ConnectionPools.getInstance().checkOutAlive(siteId);

                DdrGetsEntry ddr = new DdrGetsEntry(cxn);
                ddr.File = vistaFile;
                ddr.Iens = iens;
                ddr.Fields = flds;
                ddr.Flags = flags;

                String[] result = null;
                result = ddr.execute();
                return result;
            }
            catch (UnauthorizedAccessException uae)
            {
                logging.Log.LOG("Connection not properly authenticated: " + uae.Message);
                cxn.disconnect(); // will cause cxn pool to discard
                throw uae;
            }
            catch (System.Net.Sockets.SocketException se)
            {
                logging.Log.LOG("Socket error: " + se.Message);
                cxn.disconnect(); // will cause cxn pool to discard
                throw se;
            }
            catch (gov.va.medora.mdo.exceptions.ConnectionException ce)
            {
                logging.Log.LOG("Cxn error: " + ce.Message);
                cxn.disconnect(); // will cause cxn pool to discard
                throw ce;
            }
            catch (Exception exc)
            {
                logging.Log.LOG("DDR GETS ENTRY Exception: " + exc.Message);
                throw exc;
            }
            finally
            {
                try
                {
                    ConnectionPools.getInstance().checkIn(cxn);
                }
                catch (Exception) { }
            }
        }

        public string getVariableValueQuery(string siteId, string arg)
        {
            AbstractConnection cxn = null;

            try
            {
                logging.Log.LOG(String.Format("GVV: {0}, {1}", siteId, arg));
                cxn = (AbstractConnection)ConnectionPools.getInstance().checkOutAlive(siteId);

                VistaToolsDao dao = new VistaToolsDao(cxn);
                String result = null;

                result = dao.getVariableValue(arg);
                return result;
            }
            catch (UnauthorizedAccessException uae)
            {
                logging.Log.LOG("Connection not properly authenticated: " + uae.Message);
                cxn.disconnect(); // will cause cxn pool to discard
                throw uae;
            }
            catch (System.Net.Sockets.SocketException se)
            {
                logging.Log.LOG("Socket error: " + se.Message);
                cxn.disconnect(); // will cause cxn pool to discard
                throw se;
            }
            catch (gov.va.medora.mdo.exceptions.ConnectionException ce)
            {
                logging.Log.LOG("Cxn error: " + ce.Message);
                cxn.disconnect(); // will cause cxn pool to discard
                throw ce;
            }
            catch (Exception exc)
            {
                logging.Log.LOG("GVV Exception: " + exc.Message);
                throw exc;
            }
            finally
            {
                try
                {
                    ConnectionPools.getInstance().checkIn(cxn);
                }
                catch (Exception) { }
            }
        }

        #region Chem Hem Testing
        ///// <summary>
        ///// This method is currently using a RPC to fetch this data
        ///// </summary>
        ///// <param name="dfn"></param>
        ///// <param name="fromDate">Should be format "YYYYMMDD"</param>
        ///// <param name="siteId"></param>
        ///// <returns></returns>
        //public QueryResults getChemHemResultsForPatient(String dfn, String fromDate, String siteId)
        //{
        //    AbstractConnection cxn = null;
        //    ChemHemReport[] rpts = null;

        //    try
        //    {
        //        cxn = (AbstractConnection)ConnectionPools.getInstance().checkOutAlive(siteId);

        //        // must first set CPRS
        //        //new VistaUserDao(cxn).setContext("OR CPRS GUI CHART");

        //        gov.va.medora.mdo.dao.vista.VistaChemHemDao dao = new VistaChemHemDao(cxn);
        //        rpts = dao.getChemHemReports(dfn, fromDate, DateTime.Now.ToString("yyyyMMdd"));

        //        // then need to set back to CAPRI!
        //        //new VistaUserDao(cxn).setContext("DVBA CAPRI GUI");
        //    }
        //    catch (Exception)
        //    {
        //        return null;
        //    }
        //    finally
        //    {
        //        try
        //        {
        //            ConnectionPools.getInstance().checkIn(cxn);
        //        }
        //        catch (Exception) { }
        //    }

        //    // build this up after returning cxn to the pool
        //    return toQueryResultsFromChemHemRpts(rpts, siteId, DateTime.Now);
        //}

        //internal QueryResults toQueryResultsFromChemHemRpts(ChemHemReport[] rpts, String siteId, DateTime retrievalTimestamp)
        //{
        //    QueryResults result = new QueryResults();
        //    result.DdrResults = new List<DataTable>();

        //    DataTable chemHemTable = DataTableUtils.generateVistaQueryDataTable("63.04", new String[] { ".01", ".05", ".06", ".33" }, true, null); // there needs to be more fields here!

        //    for (int i = 0; i < rpts.Length; i++)
        //    {
        //        if (rpts[i].Specimen == null) // this is where all the good stuff is!
        //        {
        //            // should log!
        //            continue; 
        //        }

        //        String pien =  ""; // this should be the LRDFN - PIEN
        //        String ien = String.Concat(rpts[i].Specimen.Id, "_", pien); 
        //        String specimenTimestamp = rpts[i].Specimen.CollectionDate; // .01
        //        String reportedDate = rpts[i].Timestamp; // .03
        //        String specimenType = rpts[i].Specimen.Name; // .05
        //        String accessionNumber = rpts[i].Specimen.AccessionNumber; // .06

        //        for (int j = 0; j < rpts[i].Results.Length; j++)
        //        {
        //            String interpretation = rpts[i].Results[j].BoundaryStatus;
        //            String refLow = rpts[i].Results[j].Test.LowRef;
        //            String refHigh = rpts[i].Results[j].Test.HiRef;
        //            String testResult = rpts[i].Results[j].Value;
        //            String testName = rpts[i].Results[j].Test.Name;
        //            String testId = rpts[i].Results[j].Test.Id;

        //            chemHemTable.Rows.Add(new object[] { pien, ien, siteId, retrievalTimestamp, rpts[i].Timestamp, rpts[i].Type, rpts[i].Specimen.AccessionNumber, rpts[i].Specimen.Facility.Id }); // map the objects out like this
        //        }
        //    }

        //    result.DdrResults.Add(chemHemTable);

        //    return result;
        //}

        #endregion
    }
}
