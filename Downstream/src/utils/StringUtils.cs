using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using com.bitscopic.downstream.domain;
using com.bitscopic.downstream.domain.data;
using com.bitscopic.downstream.domain.svc;

namespace com.bitscopic.downstream.utils
{
    public static class StringUtils
    {
        public const String DATE_FORMAT_FOR_BATCH_ID = "yyyyMMddHHmmss";
        private static readonly object _locker = new object();
        /// <summary>
        /// Obtain a new Downstream batch ID using the current system time. Not guaranteed unique if run across differenct machines or processes
        /// </summary>
        /// <param name="threadSafe">Set to true to ensure unique batch ID in this process</param>
        /// <returns>Downstream format Batch ID</returns>
        public static String getNewBatchId(bool threadSafe)
        {
            if (threadSafe)
            {
                lock (_locker)
                {
                    System.Threading.Thread.Sleep(1000); // since batch ID resolution is 1 second, always sleep at least 1 second to quarantee uniqueness
                    return DateTime.Now.ToString(DATE_FORMAT_FOR_BATCH_ID);
                }
            }
            else
            {
                return DateTime.Now.ToString(DATE_FORMAT_FOR_BATCH_ID);
            }
        }

        // Replace date expressions with fileman dates
        // Only works for years>2000
        internal static string replaceDateExpressions(string expression)
        {
            try
            {
                string pattern = @"(DAY|WEEK|MONTH|YEAR){1}[-|+]{1}\d{1,2}";
                //Match match = Regex.Match(expression, pattern);
                MatchCollection matches = Regex.Matches(expression, pattern);
                foreach (Match match in matches)
                {
                    if (match.Success)
                    {
                        int toggle = -1;
                        DateTime now = DateTime.Now;
                        string[] parts = match.Value.Split('-');
                        if (parts.Length == 1)
                        {
                            parts = match.Value.Split('+');
                            toggle = 1;
                        }
                        string dwmy = parts[0]; int cnt = Convert.ToInt32(parts[1]);
                        switch (dwmy)
                        {
                            case "DAY":
                                now = now.AddDays(1 * cnt * toggle);
                                break;
                            case "WEEK":
                                now = now.AddDays(7 * cnt * toggle);
                                break;
                            case "MONTH":
                                now = now.AddMonths(1 * cnt * toggle);
                                break;
                            case "YEAR":
                                now = now.AddYears(1 * cnt * toggle);
                                break;
                        }
                        //expression = Regex.Replace(expression, , "3" + now.ToString("yyMMdd"));
                        expression = expression.Replace(match.Value, "3" + now.ToString("yyMMdd"));
                    }
                }
            }
            catch (Exception exc)
            {
                //Report.addError(exc.Message, exc);
                //((OrchestratorReport)Report).HasError = "T";
            }
            return expression;
        }

        internal static String convertToVistaDate(DateTime dt)
        {
            return String.Concat("3", dt.ToString("yyMMdd"));
        }

        internal static String getVistaDate()
        {
            return String.Concat("3", DateTime.Now.ToString("yyMMdd"));
        }

        internal static String getPastVistaDate(Int32 daysToSubtract)
        {
            return String.Concat("3", DateTime.Now.Subtract(new TimeSpan(daysToSubtract, 0, 0, 0)).ToString("yyMMdd"));
        }

        internal static String getFutureVistaDate(Int32 daysToAdd)
        {
            return String.Concat("3", DateTime.Now.Add(new TimeSpan(daysToAdd, 0, 0, 0)).ToString("yyMMdd"));
        }

        internal static String getInverseVistaDate()
        {
            Int32 inverseDate = 9999999 - Convert.ToInt32(String.Concat("3", DateTime.Now.ToString("yyMMdd")));
            return inverseDate.ToString();
        }

        internal static String getPastInverseVistaDate(Int32 daysToSubtract)
        {
            Int32 inverseDate = 9999999 - Convert.ToInt32(String.Concat("3", DateTime.Now.Subtract(new TimeSpan(daysToSubtract, 0, 0, 0)).ToString("yyMMdd")));
            return inverseDate.ToString();
        }

        internal static String getFutureInverseVistaDate(Int32 daysToAdd)
        {
            Int32 inverseDate = 9999999 - Convert.ToInt32(String.Concat("3", DateTime.Now.Add(new TimeSpan(daysToAdd, 0, 0, 0)).ToString("yyMMdd")));
            return inverseDate.ToString();
        }

        internal static bool isRootNode(String fullyQualifiedFile)
        {
            if (String.IsNullOrEmpty(fullyQualifiedFile))
            {
                return false; // should I throw an exception here?
            }
            if (fullyQualifiedFile.Where(s => s == '/').Count() == 1)
            {
                return true;
            }
            return false;
        }

        /// <summary>
        /// Convert a string: ,topLevelIEN,secondLevelIEN,thirdLevelIEN, to: topLevelIEN_secondLevelIEN_thirdLevelIEN
        /// </summary>
        /// <param name="iensString"></param>
        /// <returns></returns>
        public static String convertIensToIen(String iensString)
        {
            if (String.IsNullOrEmpty(iensString))
            {
                return String.Empty;
            }

            if (!iensString.Contains(",") || !iensString.EndsWith(","))
            {
                throw new ArgumentException(String.Format("Invalid IENS string: {0}", iensString));
            }

            if (iensString.StartsWith(","))
            {
                iensString = iensString.Substring(1, iensString.Length - 1); // remove first comma if present
            }
            iensString = iensString.Substring(0, iensString.Length - 1); // remove last comma
            return iensString.Replace(',', '_');
        }

        // this function should be fast! benchmarking shows 100K iterations in ~1-1.5 seconds - not bad!
        public static String stripChars(String theString, IList<char> charsToRemove)
        {
            bool foundOne = false;

            foreach (Char c in charsToRemove)
            {
                if (theString.Contains(c))
                {
                    foundOne = true;
                    break;
                }
            }
            if (!foundOne)
            {
                return theString; // return string unchanged and not re-encoded
            }
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < theString.Length; i++)
            {
                bool charIsValid = true;
                foreach (Char charToRemove in charsToRemove)
                {
                    if (theString[i] == charToRemove)
                    {
                        //System.Console.WriteLine("Found an invalid char!");
                        charIsValid = false;
                        break; 
                    }
                }
                if (charIsValid)
                {
                    sb.Append(theString[i]); 
                }
            }
            return sb.ToString();
        }

        public static Tree<QueryConfiguration> getTreeFromJson(String jsonQueryConfigurationTreeTO)
        {
            QueryConfigurationTreeTO deserialized = gov.va.medora.utils.JsonUtils.Deserialize<QueryConfigurationTreeTO>(jsonQueryConfigurationTreeTO);

            Tree<QueryConfiguration> tree = new Tree<QueryConfiguration>(new TreeNode<QueryConfiguration>(getQueryConfigurationFromTO(deserialized.rootNode)));

            if (deserialized.rootNode.children != null && deserialized.rootNode.children.Length > 0)
            {
                for (int i = 0; i < deserialized.rootNode.children.Length; i++)
                {
                    addChildRecursive(tree.RootNode, deserialized.rootNode.children[i]);
                }
            }

            return tree;
        }

        static void addChildRecursive(TreeNode<QueryConfiguration> parent, QueryConfigurationTreeNodeTO childToTranslate)
        {
            TreeNode<QueryConfiguration> child = new TreeNode<QueryConfiguration>(getQueryConfigurationFromTO(childToTranslate));
            parent.addChild(child);
            if (childToTranslate.children != null && childToTranslate.children.Length > 0)
            {
                for (int i = 0; i < childToTranslate.children.Length; i++)
                {
                    addChildRecursive(child, childToTranslate.children[i]);
                }
            }
        }

        static QueryConfiguration getQueryConfigurationFromTO(QueryConfigurationTreeNodeTO treeTO)
        {
            QueryConfiguration result = new QueryConfiguration()
            {
                Fields = treeTO.value.fields,
                File = treeTO.value.file,
                From = treeTO.value.from,
                FullyQualifiedFile = treeTO.value.fullyQualifiedFile,
                Gets_Alignment = treeTO.value.getsAlignment,
                HasChildren = treeTO.value.hasChildren,
                IdentifiedFiles = treeTO.value.identifiedFiles,
                Identifier = treeTO.value.identifier,
                Packed = treeTO.value.packed,
                Part = treeTO.value.part,
                Screen = treeTO.value.screen,
                WP_OR_COMPUTED_FIELDS = treeTO.value.wpOrComputedFields,
                XREF = treeTO.value.xref
            };

            return result;
        }

        internal static String vistaQueryToString(VistaQuery vq)
        {
            StringBuilder sb = new StringBuilder();
            return sb.ToString();
        }

        internal static String ddrListerToString(String siteId, string vistaFile, string iens, string fields, string flags, string maxRex, string from, string part, string xRef, string screen, string identifier)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(siteId);
            sb.Append("_");
            sb.Append(vistaFile);
            sb.Append("_");
            sb.Append(iens);
            sb.Append("_");
            sb.Append(fields);
            sb.Append("_");
            sb.Append(flags);
            sb.Append("_");
            sb.Append(maxRex);
            sb.Append("_");
            sb.Append(from);
            sb.Append("_");
            sb.Append(part);
            sb.Append("_");
            sb.Append(xRef);
            sb.Append("_");
            sb.Append(screen);
            sb.Append("_");
            sb.Append(identifier);
            return sb.ToString();
        }

        internal static String ddrGetsEntryToString(String siteId, string iens, string flds, string flags)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(siteId);
            sb.Append("_");
            sb.Append(iens);
            sb.Append("_");
            sb.Append(flds);
            sb.Append("_");
            sb.Append(flags);
            return sb.ToString();
        }

        internal static String gvvToString(String siteId, String arg)
        {
            return String.Concat(siteId, "_", arg);
        }

        public static String Serialize<T>(T obj)
        {
            return Newtonsoft.Json.JsonConvert.SerializeObject(obj);
            // didn't run any serialization tests but benchmarking shows Newtonsoft library to be about 4 times faster than .NET for deserialization!!!!
            //DataContractJsonSerializer serializer = new DataContractJsonSerializer(obj.GetType());
            //MemoryStream ms = new MemoryStream();
            //serializer.WriteObject(ms, obj);
            //string retVal = Encoding.UTF8.GetString(ms.ToArray());
            //return retVal;
        }

        public static T Deserialize<T>(String json)
        {
            return Newtonsoft.Json.JsonConvert.DeserializeObject<T>(json);
            // benchmarking shows Newtonsoft library to be about 4 times faster than .NET!!!!
            //T obj = Activator.CreateInstance<T>();
            //MemoryStream ms = new MemoryStream(Encoding.Unicode.GetBytes(json));
            //DataContractJsonSerializer serializer = new DataContractJsonSerializer(obj.GetType());
            //obj = (T)serializer.ReadObject(ms);
            //ms.Close();
            //return obj;
        }

        public static KeyValuePair<String, String> findLargestGap(IList<String> collection)
        {
            IList<Int64> sorted = sortStringsByNumber(collection);

            Int64 from = 0;
            Int64 to = 0;
            Int64 thisConversion = 0;
            Int64 lastConversion = 0;
            Int64 largestGap = 0;
            for (int i = 0; i < sorted.Count; i++)
            {
                if (i + 1 >= sorted.Count)
                {
                    break;
                }
                thisConversion = sorted[i];
                lastConversion = sorted[i + 1];

                if (Math.Abs(lastConversion - thisConversion) > largestGap)
                {
                    largestGap = Math.Abs(lastConversion - thisConversion);
                    from = sorted[i];
                    to = sorted[i + 1];
                }
            }
            return new KeyValuePair<string, string>(from.ToString(), to.ToString());
        }

        public static IList<Int64> sortStringsByNumber(IList<String> listOfNumbers)
        {
            List<Int64> listToSort = new List<Int64>();
            for (int i = 0; i < listOfNumbers.Count; i++)
            {
                listToSort.Add(Convert.ToInt64(Math.Floor(Convert.ToDecimal(listOfNumbers[i]))));
            }

            listToSort.Sort();

            //for (int i = 0; i < listOfNumbers.Count; i++)
            //{
            //    listOfNumbers[i] = listToSort[i].ToString();
            //}

            return listToSort;
        }
    }
}
