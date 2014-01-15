using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;
using com.bitscopic.downstream.domain;
using com.bitscopic.downstream.domain.data;
using com.bitscopic.downstream.service;

namespace com.bitscopic.downstream.dao.vista
{
    /// <summary>
    /// This utility class helps to locate the appropriate IEN starting point for an extraction. In most cases,
    /// it is not a requirement to extract ALL data from a Vista file. It may be that only data created since 1/1/2010 
    /// should be extracted. In some cases, it's not possible to specify a date range for a Vista file (e.g. PATIENT
    /// file #2 does not have any "created on" or "last accessed" fields). Fortunately, many of the Vista files
    /// that do have fields that can be used for this purpose. The functions below illustrate using a date parameter
    /// to find the IEN starting point and then returning that IEN so the Extractor can traverse the file normally
    /// </summary>
    public class VistaIenSeeder
    {
        internal static IList<String> seedList = new List<String>() { "52", "9000010", "9000010.07", "9000010.18", "405", "356", "120.5" };
        static String _seedStartDate = "3100101"; // default but can be changed in config
        static Int32 _labMicroSeedOverlap = 60; // default number of days to overlap for files lab micro (63.05) 
        static Int32 _pharmacy55Overlap = 30; // default number of days to overlap for files pharmacy unit dose (55.06) 
        IVistaDao _dao;
        

        public VistaIenSeeder(IVistaDao dao)
        {
            _dao = dao;
            if (!String.IsNullOrEmpty(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SeedStartDate]))
            {
                VistaIenSeeder._seedStartDate = ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SeedStartDate];
                //_dao.getReport().addDebug("Found seeding date in config file: " + VistaIenSeeder.seedStartDate);
            }
            if (!String.IsNullOrEmpty(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SeedOverlapDaysLabMicro]))
            {
                Int32.TryParse(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SeedOverlapDaysLabMicro], out _labMicroSeedOverlap);
            }
            if (!String.IsNullOrEmpty(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SeedOverlapDaysPharm55]))
            {
                Int32.TryParse(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.SeedOverlapDaysPharm55], out _pharmacy55Overlap);
            }
        }


        public bool needsSeeding(String vistaFile)
        {
            return VistaIenSeeder.seedList.Contains(vistaFile);
        }

        public String getSeed(String sitecode, String vistaFile)
        {
            try
            {
                switch (vistaFile)
                {
                    case "120.5":
                        return get120x5Seed(sitecode);
                    case "52":
                        return get52Seed(sitecode);
                    case "9000010":
                        return get9000010Seed(sitecode);
                    case "9000010.07":
                        return get9000010x07Seed(sitecode);
                    case "9000010.18":
                        return get9000010x18Seed(sitecode);
                    case "356":
                        return get356Seed(sitecode);
                    case "405":
                        return get405Seed(sitecode);
                    default:
                        throw new ArgumentException("No seed configuration has been made for that file!");
                }
            }
            catch (IndexOutOfRangeException) // some sites do not have data in all these files (e.g. 757 has no data in 405) so getXSeed below errors - just return empty string and handle at higher level
            {
                return "";
            }
            catch (ArgumentException)
            {
                throw;
            }
        }

        internal string get120x5Seed(string sitecode)
        {
            String[] results = _dao.ddrLister(sitecode, "120.5", "", ".01", "IP", "1", VistaIenSeeder._seedStartDate, "", "B", "", "");
            String startIen = results[0].Split(new char[] { '^' })[0];
            //_dao.getReport().addDebug(String.Format("Seeding query for file {0} at site {1} - XREF:{2}, Result:{3}", "405", sitecode, "B", startIen));
            return startIen;
        }

        internal string get405Seed(string sitecode)
        {
            String[] results = _dao.ddrLister(sitecode, "405", "", ".01", "IP", "1", VistaIenSeeder._seedStartDate, "", "B", "", "");
            String startIen = results[0].Split(new char[] { '^' })[0];
            //_dao.getReport().addDebug(String.Format("Seeding query for file {0} at site {1} - XREF:{2}, Result:{3}", "405", sitecode, "B", startIen));
            return startIen;
        }

        internal string get356Seed(string sitecode)
        {
            String[] results = _dao.ddrLister(sitecode, "356", "", ".01", "IP", "1", VistaIenSeeder._seedStartDate, "", "D", "", "");
            String startIen = results[0].Split(new char[] { '^' })[0];
            //_dao.getReport().addDebug(String.Format("Seeding query for file {0} at site {1} - XREF:{2}, Result:{3}", "356", sitecode, "D", startIen));
            return startIen;
        }

        internal string get9000010x18Seed(string sitecode)
        {
            String seed9000010 = get9000010Seed(sitecode); // no way to query 9000010.18 by date directly but there is a xref on visit so we use that as a best estimate
            String[] results = _dao.ddrLister(sitecode, "9000010.18", "", ".01", "IP", "1", seed9000010, "", "AD", "", "");
            String startIen = results[0].Split(new char[] { '^' })[0];
            //_dao.getReport().addDebug(String.Format("Seeding query for file {0} at site {1} - XREF:{2}, Result:{3}", "9000010.18", sitecode, "AD", startIen));
            return startIen;
        }

        internal string get9000010x07Seed(string sitecode)
        {
            String seed9000010 = get9000010Seed(sitecode); // no way to query 9000010.07 by date directly but there is a xref on visit so we use that as a best estimate
            String[] results = _dao.ddrLister(sitecode, "9000010.07", "", ".01", "IP", "1", seed9000010, "", "AD", "", "");
            String startIen = results[0].Split(new char[] { '^' })[0];
            //_dao.getReport().addDebug(String.Format("Seeding query for file {0} at site {1} - XREF:{2}, Result:{3}", "9000010.07", sitecode, "AD", startIen));
            return startIen;
        }

        internal string get9000010Seed(string sitecode)
        {
            String[] results = _dao.ddrLister(sitecode, "9000010", "", ".01", "IP", "1", VistaIenSeeder._seedStartDate, "", "B", "", "");
            String startIen = results[0].Split(new char[] { '^' })[0];
            //_dao.getReport().addDebug(String.Format("Seeding query for file {0} at site {1} - XREF:{2}, Result:{3}", "9000010", sitecode, "B", startIen));
            return startIen;
        }

        internal String get52Seed(String sitecode)
        {
            String[] results = _dao.ddrLister(sitecode, "52", "", ".01", "IP", "1", VistaIenSeeder._seedStartDate, "", "AC", "", "");
            String startIen = results[0].Split(new char[] { '^' })[0];
            //_dao.getReport().addDebug(String.Format("Seeding query for file {0} at site {1} - XREF:{2}, Result:{3}", "52", sitecode, "AC", startIen));
            return startIen;
        }
    }
}
