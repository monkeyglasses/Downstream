using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using com.bitscopic.downstream.utils;
using gov.va.medora.mdo.domain.pool.connection;
using gov.va.medora.mdo.dao;
using System.Collections.Concurrent;
using System.Configuration;

namespace com.bitscopic.downstream.dao.vista
{
    public class MockVistaDao : IVistaDao
    {
        static readonly object _locker = new object();
        static ConcurrentDictionary<String, object> _cachedResults;

        public MockVistaDao()
        {
            lock (_locker)
            {
                if (_cachedResults == null)
                {
                    _cachedResults = new file.FileDao(false).load<ConcurrentDictionary<string, object>>(ConfigurationManager.AppSettings["CachedVistaDaoFile"]);
                }
            }
        }


        /// <summary>
        /// Fetch cached DDR LISTER results using the site ID and Vista file to specify the file from which to load
        /// </summary>
        /// <param name="siteId"></param>
        /// <param name="vistaFile"></param>
        /// <param name="iens"></param>
        /// <param name="fields"></param>
        /// <param name="flags"></param>
        /// <param name="maxRex"></param>
        /// <param name="from"></param>
        /// <param name="part"></param>
        /// <param name="xRef"></param>
        /// <param name="screen"></param>
        /// <param name="identifier"></param>
        /// <returns></returns>
        public string[] ddrLister(string siteId, string vistaFile, string iens, string fields, string flags, string maxRex, string from, string part, string xRef, string screen, string identifier)
        {
            doCheckOutAndWait(siteId);
            return (String[])_cachedResults[StringUtils.ddrListerToString(siteId, vistaFile, iens, fields, flags, maxRex, from, part, xRef, screen, identifier)];
        }

        /// <summary>
        /// Fetch cached DDR GETS ENTRY using the site ID and Vista file to specify the file from which to load
        /// </summary>
        /// <param name="siteId"></param>
        /// <param name="vistaFile"></param>
        /// <param name="iens"></param>
        /// <param name="flds"></param>
        /// <param name="flags"></param>
        /// <returns></returns>
        public string[] ddrGetsEntry(string siteId, string vistaFile, string iens, string flds, string flags)
        {
            doCheckOutAndWait(siteId);
            return (String[])_cachedResults[StringUtils.ddrGetsEntryToString(siteId, iens, flds, flags)];
        }

        /// <summary>
        /// Fetch cached GET VARIABLE VALUE results using the site ID and Vista file to specify the file from which to load
        /// </summary>
        /// <param name="siteId"></param>
        /// <param name="arg"></param>
        /// <returns></returns>
        public string getVariableValueQuery(string siteId, string arg)
        {
            doCheckOutAndWait(siteId);
            return (String)_cachedResults[StringUtils.gvvToString(siteId, arg)];
        }

        void doCheckOutAndWait(String siteId)
        {
            AbstractConnection cxn = null;

            try
            {
                cxn = (AbstractConnection)ConnectionPools.getInstance().checkOutAlive(siteId); // we want real cxns even though we aren't really doing anything with them
                System.Threading.Thread.Sleep(1000); // simulate a small amount of wait but don't actually make any calls to Vista...
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                ConnectionPools.getInstance().checkIn(cxn);
            }
        }
    }
}
