using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using com.bitscopic.downstream.domain;
using System.Data;

namespace com.bitscopic.downstream.utils
{
    public class DataCleaningUtils
    {
        public void postProcess(QueryResults qr, ExtractorConfiguration config)
        {
            qr = QueryResultsUtils.cleanupTicket76(qr, qr.LabChemIens); // strip top level file records per ticket #76
            qr = QueryResultsUtils.cleanupTicket85(qr); // change "&amp;" to just "&" in lab accession number per ticket #85

        }

        internal bool checkTicket15(QueryResults qr, ExtractorConfiguration config)
        {
            if (!String.Equals("612", config.SiteCode) && !String.Equals("648", config.SiteCode)) // bug only encountered in 612 so far - check that site only! - found another case in files 200 and 55 in Portland!
            {
                return false;
            }
            if (!String.Equals("63", config.QueryConfigurations.RootNode.Value.File) &&
                !String.Equals("200", config.QueryConfigurations.RootNode.Value.File) &&
                !String.Equals("55", config.QueryConfigurations.RootNode.Value.File)) // next, bug only encountered in file 63 in sites 612 - check that file only! - found another case in files 200 and 55 in Portland!
            {
                return false;
            }
            bool foundRowWithZero = false;
            for (int i = qr.DdrResults[0].Rows.Count; i > 0; i--) // work backwards so as not to screw up indices
            {
                if (String.Equals(qr.DdrResults[0].Rows[i - 1][0] as String, "0"))
                {
                    foundRowWithZero = true;
                    qr.DdrResults[0].Rows.RemoveAt(i - 1); // remove all rows with "0" IEN
                }
            }

            if (foundRowWithZero && String.Equals("648", config.SiteCode) && String.Equals("200", config.QueryConfigurations.RootNode.Value.File)) // also found weird IENs in this file/site - let's remove them too while we're at it!
            {
                for (int i = qr.DdrResults[0].Rows.Count; i > 0; i--) // work backwards so as not to screw up indices
                {
                    if (String.Equals(qr.DdrResults[0].Rows[i - 1][0] as String, "648")) // why is this IEN in this bunch of records??? i have no idea... let's remove it though
                    {
                        foundRowWithZero = true;
                        qr.DdrResults[0].Rows.RemoveAt(i - 1); // remove all rows with "648" IEN
                    }
                }
            }

            if (foundRowWithZero)
            {
                qr.Report.addInfo("Found 0 IEN in results! This issue was reported in ticket #15 in Assembla. For now, treating this as a complete file traversal and signaling complete");
                return true;
            }
            return false;
            // TODO - see ticket 15 
            // return true if site = 612 and file = 63 and received goofy results illustrated in ticket
        }

        /// <summary>
        /// Per ticket #119
        /// </summary>
        /// <param name="qr"></param>
        /// <param name="ec"></param>
        /// <returns></returns>
        internal QueryResults trimPatientToEndIen(QueryResults qr, ExtractorConfiguration ec)
        {
            if (String.IsNullOrEmpty(ec.EndIen) || qr.DdrResults == null || qr.DdrResults.Count == 0 || qr.DdrResults[0].Rows.Count == 0)
            {
                return qr;
            }

            Decimal configStopIen = Convert.ToDecimal(ec.EndIen);
            Decimal lastIen = Convert.ToDecimal(qr.DdrResults[0].Rows[qr.DdrResults.Count - 1]["IEN"]);
            if (lastIen >= configStopIen)
            {
                DataTable beforeTable = qr.DdrResults[0];
                DataTable afterTable = DataTableUtils.generateVistaQueryDataTable(beforeTable.TableName, ec.QueryConfigurations.RootNode.Value.Fields.Split(new char[] { ';' }), false, null);

                for (int i = 0; i < beforeTable.Rows.Count; i++)
                {
                    Decimal currentRowIen = Convert.ToDecimal(beforeTable.Rows[i]["IEN"]);
                    if (currentRowIen >= configStopIen) // don't copy stop IEN
                    {
                        break;
                    }
                    afterTable.Rows.Add(beforeTable.Rows[i].ItemArray);
                }

                qr.DdrResults[0] = afterTable; // set table to new table
            }
            // TODO - trim appointments that now have zombie parent

            return qr;
        }

        /// <summary>
        /// Compare the IENs from SQL with the IENs from Vista. Return a KVP where the key
        /// is the smallest IEN in Vista not found in SQL and the value is the greatest IEN
        /// found in Vista but not found in SQL
        /// </summary>
        /// <param name="iensFromSql"></param>
        /// <param name="iensFromVista"></param>
        /// <returns></returns>
        internal KeyValuePair<String, String> getExtractRange(IList<String> iensFromSql, IList<String> iensFromVista)
        {
            IEnumerable<String> diff = iensFromVista.Except(iensFromSql);

            if (diff.Count() == 0)
            {
                return new KeyValuePair<string,string>("0", "0");
            }

            IEnumerable<Decimal> iensAsDecimals = diff.Select(s => Decimal.Parse(s)).ToList();

            //Decimal min = iensAsDecimals.Min();
            //Decimal max = iensAsDecimals.Max();
            //// massage the lists a tad so the correct IENs are returned for the Vista config
            //iensAsDecimals = iensAsDecimals.Except(new Decimal[] { min }); // remove the min from the list
            //iensAsDecimals = iensAsDecimals.Except(new Decimal[] { max }); // remove the max from the list

            // subtract one because Vista won't pull the start IEN and that one is missing
            return new KeyValuePair<string, string>((iensAsDecimals.Min() - 1).ToString(), iensAsDecimals.Max().ToString());
        }

        public static bool lastRowHasZeroIEN(DataTable table)
        {
            if (table == null || table.Rows.Count == 0)
            {
                return false;
            }
            return String.Equals((String)table.Rows[table.Rows.Count - 1]["IEN"], "0");
        }

        public static DataTable removeRecordsWithZeroIen(DataTable table)
        {
            DataTable destination = table.Clone();
            destination.Rows.Clear();
            for (int i = 0; i < table.Rows.Count; i++)
            {
                if (Convert.ToDecimal(table.Rows[i]["IEN"]) == 0)
                {
                    continue;
                }
                object[] vals = table.Rows[i].ItemArray;
                destination.Rows.Add(vals);
            }
            return destination;
        }

        public static DataTable removeRecordsAfterLastIen(DataTable table, String lastIen)
        {
            DataTable destination = table.Clone();
            destination.Rows.Clear();
            Decimal stopPoint = Convert.ToDecimal(lastIen);
            for (int i = 0; i < table.Rows.Count; i++)
            {
                if (Convert.ToDecimal(table.Rows[i]["IEN"]) > stopPoint)
                {
                    break;
                }
                object[] vals = table.Rows[i].ItemArray;
                destination.Rows.Add(vals);
            }
            return destination;
        }

    }
}
