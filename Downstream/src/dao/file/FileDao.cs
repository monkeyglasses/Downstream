using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using System.Text;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Security.Cryptography;
using com.bitscopic.downstream.domain.exception;
using com.bitscopic.downstream.service;
using com.bitscopic.downstream.utils;
using System.IO.Compression;

namespace com.bitscopic.downstream.dao.file
{
    /// <summary>
    /// FileDao class for file based I/O on the saved Vista file tables
    /// </summary>
    public class FileDao : IFileDao
    {
        private static string ENCRYPTION_PASSWORD = "AYBABTU!"; // All Your Bases Are Belong To Us!
        private static byte[] ENCRYPTION_KEY_BYTES = new UnicodeEncoding().GetBytes(ENCRYPTION_PASSWORD);
        
        // we can use this to make sure only one thread at a time is extarcting a 7zip file
        static readonly object _locker = new object();

        // default directories - can be overridden in the app.config
        internal string BASE_DIRECTORY = "C:\\Downstream\\";
        internal string EXTRACTS_DIRECTORY;
        internal string TRIGGER_DIRECTORY;
        internal string INFORMATICA_ARCHIVE_DIRECTORY;

        internal string TRIGGER_INTERNAL_PATH = "/Exchange/Downstream/TESTING/source/";

        public string DownstreamBaseDirectory { get { return BASE_DIRECTORY; } }
        public string DownstreamExtractsDirectory { get { return EXTRACTS_DIRECTORY; } }

        String _subDir;
        String _sitecode;

        /// <summary>
        /// As a file is deserialized, it is added to this list
        /// </summary>
        public IList<FileInfo> OpenedFiles 
        {
            get
            {
                if (_openedFiles == null)
                {
                    _openedFiles = new List<FileInfo>();
                }
                return _openedFiles;
            }
            set { _openedFiles = value; }
        }
        IList<FileInfo> _openedFiles;

        /// <summary>
        /// As a file is saved, it is added to this list
        /// </summary>
        public IList<FileInfo> SavedFiles
        {
            get
            {
                if (_savedFiles == null)
                {
                    _savedFiles = new List<FileInfo>();
                }
                return _savedFiles;
            }
            set { _savedFiles = value; }
        }
        IList<FileInfo> _savedFiles;

        Int32 _savedFilesTotalBytes = 0;
        /// <summary>
        /// The disk space used by all the saved files combined
        /// </summary>
        public Int32 SavedFilesTotalBytes 
        { 
            get { return _savedFilesTotalBytes; } 
            set { _savedFilesTotalBytes = value;} 
        }

        /// <summary>
        /// As a file is deleted, it is added to this list
        /// </summary>
        public IList<FileInfo> DeletedFiles
        {
            get
            {
                if (_deletedFiles == null)
                {
                    _deletedFiles = new List<FileInfo>();
                }
                return _deletedFiles;
            }
            set { _deletedFiles = value; }
        }
        IList<FileInfo> _deletedFiles;

        /// <summary>
        /// FileDao constructor. Checks that default folders exist and creates them if not
        /// </summary>
        /// <exception cref="Exception" />
        /// <example>
        /// try
        /// {
        ///     FileDao fileDao = new FileDao();
        /// }
        /// catch (Exception exc)
        /// {
        ///     LOG.Error("This is an unrecoverable error for this software. Should exit...", exc);
        ///     Environment.Exit(1);
        /// }
        /// </example>
        public FileDao()
        {
            initializeDirectories(true);
        }

        public FileDao(bool initializeFileDirectories)
        {
            initializeDirectories(initializeFileDirectories);
        }

        void initializeDirectories(bool initialize)
        {
            if (!String.IsNullOrEmpty(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.TriggerInternalInformaticaPath]))
            {
                TRIGGER_INTERNAL_PATH = ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.TriggerInternalInformaticaPath];
            }
            // if the file path is in the config
            string configFilePath = ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.DownstreamFilePath];
            if (!String.IsNullOrEmpty(configFilePath))
            {
                BASE_DIRECTORY = configFilePath;
            }
            if (!BASE_DIRECTORY.EndsWith("\\"))
            {
                BASE_DIRECTORY = BASE_DIRECTORY + "\\";
            }

            // seems we have some edge cases where extracts are being placed in weird locations... not sure why... so, for now, setting default EXTRACTS_DIR to BASE_DIR at instantiation
            EXTRACTS_DIRECTORY = BASE_DIRECTORY;
            TRIGGER_DIRECTORY = String.Concat(BASE_DIRECTORY + "trigger\\");
            //
            if (initialize)
            {
                DirectoryInfo di = new DirectoryInfo(BASE_DIRECTORY);
                if (!di.Exists)
                {
                    try
                    {
                        di.Create();
                    }
                    catch (Exception exc)
                    {
                        throw new ApplicationException("The " + BASE_DIRECTORY +
                            " does not exist and could not be created by this application. See the inner exception for more details", exc);
                    }
                }
            }
        }

        public void createMarkerFile()
        {
            if (String.IsNullOrEmpty(EXTRACTS_DIRECTORY))
            {
                throw new ArgumentNullException("The file directory does not appear to have been set correctly!");
            }
            // we're saving the FILE_DIRECTORY string as the file contents
            String headersPathAndFlags = String.Concat(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.TriggerHeader], "\r\n"); // "PATH,PATIENT/ADT,LAB MICRO,PHARMACY,BCMA,VITALS\r\n";
            String informaticaTriggerPath = String.Concat(TRIGGER_INTERNAL_PATH, _sitecode, "/", _subDir, "/"); // e.g. /Exchange/Downstream/TESTING/source/640/20130329125959/
            headersPathAndFlags = String.Concat(headersPathAndFlags, informaticaTriggerPath, ",", ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.TriggerFlags]); // ",1,1,1,1,0"); // TODO - THESE FLAGS SHOULD NOT BE STATIC!!!!!! Need to implement the logic to set these flags based off the extraction status
            saveToFile(headersPathAndFlags, TRIGGER_DIRECTORY + _sitecode + "_" + _subDir + ".csv");
            // File: (name: \\base_dir\trigger\506_20130329125959.csv)
            //-----------------------------------------------------------------------
            //|PATH,D1,D2,D3,etc                                                    |
            //|/Exchange/Downstream/TESTING/source/640/20130329125959/,1,1,1,etc    |
            //|                                                                     |
            //|                                                                     |
            //|                                                                     |
            //-----------------------------------------------------------------------
        }

        public void createTriggerForSiteAndBatch(String triggerDirectory, String tempArchivePathUnixFormat, String siteId, String downstreamBatchId, String[] flags)
        {
            // we're saving the FILE_DIRECTORY string as the file contents
            String headersPathAndFlags = "PATH,PATIENT/ADT,LAB MICRO,PHARMACY,BCMA,VITALS\r\n";


            headersPathAndFlags = String.Concat(headersPathAndFlags, tempArchivePathUnixFormat);
            for (int i = 0; i < flags.Length; i++)
            {
                headersPathAndFlags = String.Concat(headersPathAndFlags, ",", flags[i]);
            }
            saveToFile(headersPathAndFlags, triggerDirectory + siteId + "_" + downstreamBatchId + ".csv");
        }

        public void setExtractsDirectory(String sitecode, String subDirectory)
        {
            _sitecode = sitecode;
            _subDir = subDirectory;
            // should be something like: \\BASE_DIR\source\506\20130101123005  where timestamp is specified by orchestrator with new job request
            DirectoryInfo di = new DirectoryInfo(BASE_DIRECTORY + "source\\" + sitecode + "\\" + subDirectory);
            if (!di.Exists)
            {
                try
                {
                    di.Create();
                }
                catch (System.Security.SecurityException)
                {
                    throw;
                }
                catch (Exception) { /* directory was probably created by another thread/process if issue wasn't with security - swallow for now */ }
            }
            EXTRACTS_DIRECTORY = di.FullName;
            if (!EXTRACTS_DIRECTORY.EndsWith("\\"))
            {
                EXTRACTS_DIRECTORY = String.Concat(EXTRACTS_DIRECTORY, "\\");
            }
        }

        //public void createDirectoryForConfig(String sitecode, String vistaFile, DateTime extractionStartTimestamp)
        //{
        //    _sitecode = sitecode;
        //    // files should be dumped in something like \\server\extracts\506_2_20130320053001
        //    DirectoryInfo diSite = new DirectoryInfo(BASE_DIRECTORY + sitecode);
        //    if (!diSite.Exists)
        //    {
        //        diSite.Create();
        //    }
        //    // should be: \\BASE_DIR\506\2_20130101123005
        //    DirectoryInfo di = new DirectoryInfo(BASE_DIRECTORY + sitecode + "\\" + vistaFile + "_" + extractionStartTimestamp.ToString("yyyyMMddHHmmss")); 
        //    if (!di.Exists)
        //    {
        //        di.Create();
        //    }
        //    EXTRACTS_DIRECTORY = di.FullName;
        //    if (!EXTRACTS_DIRECTORY.EndsWith("\\"))
        //    {
        //        EXTRACTS_DIRECTORY = String.Concat(EXTRACTS_DIRECTORY, "\\");
        //    }
        //}

        /// <summary>
        /// Delete a list of files (can pass the OpenedFiles and SavedFiles)
        /// </summary>
        /// <param name="files"></param>
        public void deleteFiles(IList<FileInfo> files)
        {
            lock (_locker)
            {
                if (files == null || files.Count == 0)
                {
                    return;
                }
                foreach (FileInfo fi in files)
                {
                    DeletedFiles.Add(fi);
                    fi.Delete();
                }
            }
        }

        /// <summary>
        /// Deletes ALL .downstream files from downstream folder. USE WITH CAUTION!!!
        /// </summary>
        public void deleteAllDownstreamFiles()
        {
            lock (_locker)
            {
                DirectoryInfo di = new DirectoryInfo(EXTRACTS_DIRECTORY);
                FileInfo[] fis = di.GetFiles("*.downstream");

                foreach (FileInfo fi in fis)
                {
                    fi.Delete();
                    DeletedFiles.Add(fi);
                }
            }
        }

        /// <summary>
        /// Delete all the files matching the pattern: "sitecode*" from the main Downstream folder
        /// </summary>
        /// <param name="sitecode">The sitecode to use in the file name match</param>
        public void deleteFilesFromSite(string sitecode)
        {
            deleteFilesFromSiteInFolder(sitecode + "_", new DirectoryInfo(EXTRACTS_DIRECTORY));
        }

        /// <summary>
        /// Delete all the files matching the pattern: "sitecode_vistaFile_*" from the main Downstream folder
        /// </summary>
        /// <param name="sitecode">The sitecode to use in the file name match</param>
        /// <param name="vistaFile">The Vista file to use in the file name match</param>
        public void deleteFilesFromSite(string sitecode, string vistaFile)
        {
            deleteFilesFromSiteInFolder(sitecode + "_" + vistaFile + "_", new DirectoryInfo(EXTRACTS_DIRECTORY));
        }

        /// <summary>
        /// Delete all the files matching the pattern: "fileNameBeginning*" from the folderPath folder
        /// </summary>
        /// <param name="fileNameBeginning">The beginning characters of the file name to match</param>
        /// <param name="folderPath">The DirectoryInfo object created from the path where the files are to be deleted</param>
        public void deleteFilesFromSiteInFolder(string fileNameBeginning, DirectoryInfo folderPath)
        {
            lock (_locker)
            {
                //DirectoryInfo di = new DirectoryInfo(folderPath);
                FileInfo[] fis = folderPath.GetFiles(fileNameBeginning + "*");
                if (fis == null || fis.Length == 0)
                {
                    return;
                }
                foreach (FileInfo fi in fis)
                {
                    fi.Delete();
                    DeletedFiles.Add(fi);
                }
            }
        }

        /// <summary>
        /// Serialize an object to a file. fileName should be the full file path.
        /// </summary>
        /// <param name="graph">The object graph to save</param>
        /// <param name="fileName">The full path file name to use for the serialization</param>
        /// <returns>The full file path of the saved file</returns>
        /// <exception cref="ArgumentException" />
        /// <exception cref="ArgumentNullException" />
        /// <exception cref="SerializationException" />
        public string saveToFile(object graph, string fileName)
        {
            return saveToFile(graph, fileName, false);
        }

        public void saveFilesWithRollback(IEnumerable<DataTable> tables, ExtractorMode extractMode, Int32 maxTries)
        {
            IList<String> createdFileNames = new List<String>();
            int tries = 0;
            //int maxTries = 10;
            int saveIdx = 0;

            while (tries < maxTries)
            {
                try
                {
                    if (saveIdx < tables.Count())
                    {
                        if (tables.ElementAt(saveIdx) == null || tables.ElementAt(saveIdx).Rows.Count == 0) // don't want saveToFile to throw an exception
                        {
                            saveIdx++;
                            continue;
                        }
                        String currentFile = saveToFile(tables.ElementAt(saveIdx), extractMode);
                        createdFileNames.Add(currentFile);
                        saveIdx++;
                        continue;
                    }
                    break;
                }
                catch (Exception)
                {
                    tries++;
                    if (tries >= maxTries)
                    {
                        deleteFilesWithRetry(createdFileNames, maxTries);
                        throw;
                    }
                    else
                    {
                        System.Threading.Thread.Sleep(60 * 1000);
                    }
                }
            }
        }

        private void deleteFilesWithRetry(IList<string> createdFileNames, Int32 maxTries)
        {
            if (createdFileNames == null || createdFileNames.Count == 0)
            {
                return;
            }
            int tries = 0;
           // int maxTries = 10;

            while (tries < maxTries)
            {
                try
                {
                    if (createdFileNames.Count > 0)
                    {
                        File.Delete(createdFileNames[0]);
                        createdFileNames.RemoveAt(0);
                        tries = 0;
                        continue; // use while loop above for looping
                    }
                    break;
                }
                catch (Exception)
                {
                    tries++;
                    if (tries >= maxTries)
                    {
                        throw new DownstreamFileTransactionException("Unable to save/delete files as transaction - this problem will need to be manually resolved!");
                    }
                    else
                    {
                        System.Threading.Thread.Sleep(60 * 1000);
                    }
                }
            }
        }

        /// <summary>
        /// Serialize an object to a file. fileName should be the full file path
        /// </summary>
        /// <param name="graph">The object graph to save</param>
        /// <param name="fileName">The full path file name to use for the serialization</param>
        /// <param name="encrypt">True of False to turn encryption on and off</param>
        /// <returns>The full file path of the saved file</returns>
        /// <exception cref="ArgumentException" />
        /// <exception cref="ArgumentNullException" />
        /// <exception cref="SerializationException" />
        public string saveToFile(object graph, string fileName, bool usingEncryption)
        {
            if (graph == null)
            {
                throw new ArgumentNullException("Must supply a valid object to save");
            }
            // check if file exists already and delete if so - no longer doing this due to overhead. filesystem should throw an error which we will catch. really is an error condition anyways
            //if (File.Exists(fileName))
            //{
            //    File.Delete(fileName);
            //}
            
            using (FileStream fs = new FileStream(fileName, FileMode.Create))
            {
                // serialize object
                BinaryFormatter formatter = new BinaryFormatter();

                // this IF block is UGLY! used as a way to save a byte[] untouched by serialization or encryption (really just in unit tests)
                if (graph is byte[] && usingEncryption == false)
                {
                    byte[] buffer = (byte[])graph;
                    fs.Write(buffer, 0, buffer.Length);
                }
                else if (graph is String && usingEncryption == false)
                {
                    byte[] temp = Encoding.UTF8.GetBytes((String)graph);
                    fs.Write(temp, 0, temp.Length);
                }

                else if (usingEncryption)
                {
                    MemoryStream ms = new MemoryStream();
                    formatter.Serialize(ms, graph);
                    RijndaelManaged crypto = new RijndaelManaged();
                    CryptoStream cs = new CryptoStream(fs, crypto.CreateEncryptor(ENCRYPTION_KEY_BYTES, ENCRYPTION_KEY_BYTES), CryptoStreamMode.Write);
                    cs.Write(ms.GetBuffer(), 0, Convert.ToInt32(ms.Length));
                }
                else if (!usingEncryption)
                {
                    formatter.Serialize(fs, graph);
                }

                fs.Flush();
                fs.Close();

                FileInfo fi = new FileInfo(fileName);
                _savedFilesTotalBytes += Convert.ToInt32(fi.Length);
                SavedFiles.Add(fi);
                return fileName;
            }

            //try
            //{
            //    // serialize object
            //    BinaryFormatter formatter = new BinaryFormatter();

            //    // this IF block is UGLY! used as a way to save a byte[] untouched by serialization or encryption (really just in unit tests)
            //    if (graph is byte[] && usingEncryption == false)
            //    {
            //        byte[] buffer = (byte[])graph;
            //        fs.Write(buffer, 0, buffer.Length);
            //    }
            //    else if (graph is String && usingEncryption == false)
            //    {
            //        byte[] temp = Encoding.UTF8.GetBytes((String)graph);
            //        fs.Write(temp, 0, temp.Length);
            //        //fs.Flush();
            //        //fs.Close();
            //    }

            //    else if (usingEncryption)
            //    {
            //        MemoryStream ms = new MemoryStream();
            //        formatter.Serialize(ms, graph);
            //        RijndaelManaged crypto = new RijndaelManaged();
            //        CryptoStream cs = new CryptoStream(fs, crypto.CreateEncryptor(ENCRYPTION_KEY_BYTES, ENCRYPTION_KEY_BYTES), CryptoStreamMode.Write);
            //        cs.Write(ms.GetBuffer(), 0, Convert.ToInt32(ms.Length));
            //        //cs.Flush();
            //        //cs.Close();
            //    }
            //    else if (!usingEncryption)
            //    {
            //        formatter.Serialize(fs, graph);
            //    }

            //    fs.Flush();

            //    FileInfo fi = new FileInfo(fileName);
            //    _savedFilesTotalBytes += Convert.ToInt32(fi.Length);
            //    SavedFiles.Add(fi);
            //    return fileName;
            //}
            //catch (SerializationException se)
            //{
            //    // TODO - NEED TO LOG THIS
            //    string reason = se.Message;
            //    throw;
            //}
            //catch (Exception)
            //{
            //    throw;
            //}
            //finally
            //{
            //    fs.Close();
            //    fs.
            //}
        }

        /// <summary>
        /// Save a table to a file. 
        /// Filename format is: sitecode_vistaFile_firstIEN_lastIEN[_NEW or _UPDATED].downstream
        /// </summary>
        /// <param name="table">The DataTable to save</param>
        /// <param name="type">Use to format the file name</param>
        /// <returns>The generated file name</returns>
        /// <exception cref="ArgumentException" />
        /// <exception cref="ArgumentNullException" />
        /// <exception cref="SerializationException" />
        public string saveToFile(DataTable table, ExtractorMode type)
        {
            if (table == null || table.Rows == null || table.Rows.Count == 0)
            {
                throw new ArgumentNullException("Must supply a valid table");
            }

            return saveToFile(DataTableUtils.convertDataTableToDelimited(table), EXTRACTS_DIRECTORY + getStandardFileNameFormat(table, type));
        }

        public string saveToFileErrored(DataTable table, ExtractorMode type)
        {
            if (table == null || table.Rows == null)
            {
                throw new ArgumentNullException("Must supply a valid table");
            }
            if (table.Rows.Count == 0) return String.Empty;

            return saveToFile(DataTableUtils.convertDataTableToDelimited(table), EXTRACTS_DIRECTORY + "errored\\" + getStandardFileNameFormat(table, type));
        }

        /// <summary>
        /// Save a table to a file. 
        /// Filename format is: sitecode_vistaFile_firstIEN_lastIEN[_NEW or _UPDATED].downstream
        /// </summary>
        /// <param name="table">The DataTable to save</param>
        /// <param name="type">Use to format the file name</param>
        /// <param name="usingEncryption">Encrypt the file on disk</param>
        /// <returns>The generated file name</returns>
        public string saveToFile(DataTable table, ExtractorMode type, bool usingEncryption)
        {
            if (table == null || table.Rows == null || table.Rows.Count == 0)
            {
                throw new ArgumentNullException("Must supply a valid table");
            }

            return saveToFile(DataTableUtils.convertDataTableToDelimited(table), EXTRACTS_DIRECTORY + getStandardFileNameFormat(table, type), usingEncryption);
        }

        public KeyValuePair<String, String> getSiteAndFileFromFileName(String filePath)
        {
            if (String.IsNullOrEmpty(filePath))
            {
                throw new ArgumentNullException("Must supply a file name");
            }

            // determine if the filePath is a full path or just the file name
            String fileNamePart = "";
            int lastDirectoryDelimiterIndex = filePath.LastIndexOf("\\");
            if (lastDirectoryDelimiterIndex <= 0)
            {
                lastDirectoryDelimiterIndex = filePath.LastIndexOf("/");
            }
            if (lastDirectoryDelimiterIndex <= 0)
            {
                fileNamePart = filePath;
            }
            else
            {
                fileNamePart = filePath.Substring(lastDirectoryDelimiterIndex);
            }

            String[] fileNamePieces = fileNamePart.Split(new char[] { '_' }, StringSplitOptions.RemoveEmptyEntries);
            if (fileNamePieces == null || fileNamePieces.Length < 4)
            {
                throw new ArgumentException("{0} is not a valid file name format", filePath);
            }

            return new KeyValuePair<string, string>(fileNamePieces[0], fileNamePieces[1]);
        }

        // ensure the list of patients is sorted by local pid and build a standard file name format using the patient values
        // all patients should be from the same site
        internal string getStandardFileNameFormat(DataTable table, ExtractorMode type)
        {
            if (table == null || table.Rows == null || table.Rows.Count == 0 || String.IsNullOrEmpty(table.TableName))
            {
                throw new ApplicationException("Must supply a non-empty list of patients to the get stardard filename function");
            }
            //string sitecode = ((Int16)table.Rows[0]["SiteCode"]).ToString(); // Get the sitecode column from the first record in the table
            string sitecode = (table.Rows[0]["SiteCode"]).ToString(); // Get the sitecode column from the first record in the table
            string vistaFile = table.TableName;
            string firstIen = (table.Rows[0]["IEN"]).ToString(); // Get the IEN from the first record in the table
            string lastIen = (table.Rows[table.Rows.Count - 1]["IEN"]).ToString(); // Get the IEN from the last record in the table

            if (String.IsNullOrEmpty(sitecode) || String.IsNullOrEmpty(vistaFile) ||
                String.IsNullOrEmpty(firstIen) || String.IsNullOrEmpty(lastIen))
            {
                throw new ArgumentException("The DataTable appears to be formatted incorrectly");
            }
            
            StringBuilder fileName = new StringBuilder();
            fileName.Append(sitecode);
            fileName.Append("_");
            fileName.Append(vistaFile);
            fileName.Append("_");
            fileName.Append(firstIen);
            fileName.Append("_");
            fileName.Append(lastIen);

            if (type == ExtractorMode.INCREMENTAL)
            {
                fileName.Append("_" + Enum.GetName(typeof(ExtractorMode), type));
            }
            else if (type == ExtractorMode.REBUILD)
            {
                fileName.Append("_" + Enum.GetName(typeof(ExtractorMode), type));
            }
            // don't change file name for FileType.ALL enum
            fileName.Append(".downstream");
            return fileName.ToString();
        }

        public DataTable loadFromFile(string fileName)
        {
            return loadFromFile(fileName, false);
        }

        /// <summary>
        /// Retrieve a saved DataTable from a saved file
        /// </summary>
        /// <param name="fileName">The path of the file</param>
        /// <returns>The DataTable loaded from the file</returns>
        public DataTable loadFromFile(string fileName, bool usingEncryption)
        {
            if (String.IsNullOrEmpty(fileName))
            {
                throw new ArgumentNullException("Must supply a file name");
            }

            DataTable results = new DataTable();

            FileStream fs = new FileStream(fileName, FileMode.Open);
            FileInfo fi = new FileInfo(fileName);

            try
            {
                String delimited = readFile(fi.FullName);
                
                results = DataTableUtils.convertDelimitedToDataTable(getSiteAndFileFromFileName(fileName).Value, delimited);
                //if (usingEncryption)
                //{
                //    RijndaelManaged crypto = new RijndaelManaged();
                //    CryptoStream cs = new CryptoStream(fs, crypto.CreateDecryptor(ENCRYPTION_KEY_BYTES, ENCRYPTION_KEY_BYTES), CryptoStreamMode.Read);
                //    BinaryFormatter formatter = new BinaryFormatter();
                //    results = (DataTable)formatter.Deserialize(cs);
                //    cs.Close();
                //}
                //else if (!usingEncryption)
                //{
                //    BinaryFormatter bf = new BinaryFormatter();
                //    results = (DataTable)bf.Deserialize(fs);
                //}
            }
            catch (Exception)
            {
                throw;
            }
            finally 
            {
                fs.Close();
            }
            return results;
        }

        /// <summary>
        /// Load all the saved DataTable objects for a site
        /// </summary>
        /// <param name="sitecode"></param>
        /// <returns></returns>
        public IList<DataTable> loadFromAllFiles(string sitecode)
        {
            IList<DataTable> newTable = loadFromAllFiles(sitecode, ExtractorMode.REBUILD);
            IList<DataTable> otherTable = loadFromAllFiles(sitecode, ExtractorMode.INCREMENTAL);
            
            List<DataTable> all = new List<DataTable>();
            all.InsertRange(0, otherTable);
            all.InsertRange(all.Count, newTable);

            return all;
        }

        /// <summary>
        /// Load all the saved DataTable objects for a site matching the saved DataType
        /// </summary>
        /// <param name="sitecode"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public IList<DataTable> loadFromAllFiles(string sitecode, ExtractorMode type)
        {
            return loadFromAllFiles(sitecode, "*", type);
        }

        /// <summary>
        /// Loads all DataTables from a site given a Vista file and matching the saved DataType
        /// </summary>
        /// <param name="sitecode">Load files from Downstream folder beginning with sitecode</param>
        /// <param name="vistaFile">Specify the Vista file to load all sitecode_vistafile_*_*_type files</param>
        /// <param name="type">Load file names ending with the DataTtype enum value</param>
        /// <returns></returns>
        public IList<DataTable> loadFromAllFiles(string sitecode, string vistaFile, ExtractorMode type)
        {
            List<DataTable> results = new List<DataTable>();
            DirectoryInfo di = new DirectoryInfo(EXTRACTS_DIRECTORY);

            try
            {
                FileInfo[] files = di.GetFiles();

                // don't want to do this in every loop since it stays the same
                string sitecodeAndVistaFile = sitecode + "_";
                if (!String.IsNullOrEmpty(vistaFile))
                {
                    sitecodeAndVistaFile += (vistaFile + "_");
                }

                foreach (FileInfo fi in files)
                {
                    bool usingThisFile = false;
                    // if a Vista file was specified, match sitecode and vista file name
                    if (!String.IsNullOrEmpty(vistaFile) && fi.Name.StartsWith(sitecodeAndVistaFile))
                    {
                        usingThisFile = true;
                    }
                    // othewise just match the sitecode
                    else if (fi.Name.StartsWith(sitecode + "_"))
                    {
                        usingThisFile = true;
                    }

                    if (usingThisFile)
                    {
                        // for each file type, if the file name doesn't end as expected then skip it
                        if (type == ExtractorMode.REBUILD && !fi.Name.Contains(Enum.GetName(typeof(ExtractorMode), type)))
                        {
                            continue;
                        }
                        if (type == ExtractorMode.INCREMENTAL && !fi.Name.Contains(Enum.GetName(typeof(ExtractorMode), type)))
                        {
                            continue;
                        }
                        //if (type == DataType.OTHER &&
                        //    (fi.Name.Contains(Enum.GetName(typeof(DataType), DataType.NEW)) ||
                        //    fi.Name.Contains(Enum.GetName(typeof(DataType), DataType.UPDATED))))
                        //{
                        //    continue;
                        //}
                        DataTable current = loadFromFile(fi.FullName);
                        results.Add(current);
                        OpenedFiles.Add(fi);
                    }
                }

            }
            catch (Exception)
            {
                throw;
            }
            
            return results;
        }

        public String readFile(String filePath)
        {
            FileInfo fi = new FileInfo(filePath);
            using (FileStream fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, Convert.ToInt32(fi.Length)))
            {
                byte[] buffer = new byte[fi.Length];
                fs.Read(buffer, 0, Convert.ToInt32(fi.Length));
                fs.Flush();
                fs.Close();
                return System.Text.Encoding.UTF8.GetString(buffer);
            }
        }

        public T load<T>(String filePath)
        {
            FileInfo fi = new FileInfo(filePath);
            using (FileStream fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, Convert.ToInt32(fi.Length)))
            {
                byte[] buffer = new byte[fi.Length];
                //fs.Read(buffer, 0, Convert.ToInt32(fi.Length));
                BinaryFormatter bf = new BinaryFormatter();
                T result = (T)bf.Deserialize(fs);
                fs.Flush();
                fs.Close();
                return result;
            }
        }

        public byte[] loadFileBytes(String filePath)
        {
            FileInfo fi = new FileInfo(filePath);
            using (FileStream fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, Convert.ToInt32(fi.Length)))
            {
                byte[] buffer = new byte[fi.Length];
                fs.Read(buffer, 0, Convert.ToInt32(fi.Length));
                fs.Flush();
                fs.Close();
                return buffer;
            }
        }

        /// <summary>
        /// Examine the standard file names to get the greatest IEN for a Vista file at a given site
        /// </summary>
        /// <param name="siteCode">The sites to look in</param>
        /// <param name="vistaFile">The Vista file to use</param>
        /// <returns>The largest IEN from a site's serialized file name</returns>
        public string getLastIen(string siteCode, string vistaFile)
        {
            return getLastFolderIen(siteCode, vistaFile, EXTRACTS_DIRECTORY);
        }

        /// <summary>
        /// sitecode_vistaFile_firstIen_lastIen[_NEW or _UPDATED].downstream
        /// </summary>
        private string getLastFolderIen(string siteCode, string vistaFile, string filePath)
        {
            DirectoryInfo di = new DirectoryInfo(filePath);
            try
            {
                FileInfo[] files = di.GetFiles();
                IList<FileInfo> siteFiles = new List<FileInfo>();
                foreach (FileInfo fi in files)
                {
                    if (fi.Name.StartsWith(siteCode))
                    {
                        siteFiles.Add(fi);
                    }
                }
                decimal greatest = 0;
                foreach (FileInfo fi in siteFiles)
                {
                    string[] pieces = fi.Name.Split(new Char[] { '_' });
                    if (pieces == null || pieces.Length < 4 || pieces[1] != vistaFile)
                    {
                        continue;
                    }
                    string lastFileIen = pieces[3];
                    decimal current = Convert.ToDecimal(lastFileIen);
                    if (current > greatest)
                    {
                        greatest = current;
                    }
                }
                return greatest.ToString();
            }
            catch (Exception)
            {
                // log!!!
                throw;
            }
        }

        /// <summary>
        /// Examine all the saved files and get a set (unique list) of SiteCode_VistaFile
        /// </summary>
        /// <returns>A set of locked files as SiteCode_VistaFile</returns>
        public ISet<string> getWorkingSites()
        {
            DirectoryInfo di = new DirectoryInfo(EXTRACTS_DIRECTORY);
            try
            {
                FileInfo[] files = di.GetFiles();
                ISet<string> results = new SortedSet<string>();
                
                foreach (FileInfo fi in files)
                {
                    int firstUnderscore = fi.Name.IndexOf('_');
                    if(!(firstUnderscore < 0))
                    {
                        int secondUnderscore = fi.Name.IndexOf('_', firstUnderscore + 1);
                        if (!(secondUnderscore < 0))
                        {
                            string compositeKey = fi.Name.Substring(0, secondUnderscore);
                            results.Add(compositeKey);
                        }
                    }
                }
                return results;
            }
            catch (Exception)
            {
                // log!!!
                throw;
            }
        }

        /// <summary>
        /// Get a random file from the downstream directory with the .downstream file extension
        /// </summary>
        /// <returns>The file name if one exists, null otherwise</returns>
        public string getRandomFile()
        {
            try
            {
                DirectoryInfo di = new DirectoryInfo(EXTRACTS_DIRECTORY);
                FileInfo[] fi = di.GetFiles("*.downstream"); // only get downstream files
                if (fi != null && fi.Length > 0)
                {
                    return fi[0].FullName;
                }
                else
                {
                    return null;
                }
            }
            catch (Exception)
            {
                return null;
            }
        }

        /// <summary>
        /// Get a list of files in the downstream working directory with a .downstream extension
        /// </summary>
        /// <returns><![CDATA[IList<FileInfo>]]></returns>
        public IList<FileInfo> getFiles()
        {
            IList<FileInfo> result = new List<FileInfo>();
            try
            {
                lock (_locker)
                {
                    DirectoryInfo di = new DirectoryInfo(EXTRACTS_DIRECTORY);
                    FileInfo[] fia = di.GetFiles("*.downstream*"); // only get downstream files
                    if (fia != null && fia.Length > 0)
                    {
                        result = new List<FileInfo>(fia);
                        return result;
                    }
                    else
                    {
                        return result;
                    }
                }
            }
            catch (Exception)
            {
                return result;
            }
        }

        // if decompressing remote zip file, must map the network drive to a local drive letter!
        public void decompressZipFile(String filePath, String destinationDirectory)
        {
            try
            {
                lock (_locker)
                {
                    System.Diagnostics.Process p = new System.Diagnostics.Process();
                    p.StartInfo.FileName = "7z.exe";
                    p.StartInfo.Arguments = " x -tzip " + filePath + " -o" + destinationDirectory + " -y";
                    p.StartInfo.UseShellExecute = false;
                    p.StartInfo.RedirectStandardOutput = true;
                    p.StartInfo.CreateNoWindow = true;
                    p.StartInfo.WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden;
                    p.Start();
                    string cmdOutput = p.StandardOutput.ReadToEnd();
                    //string errorOutput = p.StandardError.ReadToEnd();

                    //if (!String.IsNullOrEmpty(cmdOutput))
                    //{
                    //    string[] fileNames = cmdOutput.Split(new string[] { "\r\n" }, StringSplitOptions.RemoveEmptyEntries);
                    //    if (fileNames != null && fileNames.Length > 0)
                    //    {
                    //        foreach (string s in fileNames)
                    //        {
                    //            if (s.Contains("Extracting "))
                    //            {
                    //                string temp = s.Substring(s.IndexOf("  ") + 2);
                    //                SavedFiles.Add(new FileInfo(temp));
                    //                //extractedFileNames.Add(temp);
                    //                //LOG.Debug("Successfully extracted a file: " + temp);
                    //            }
                    //        }
                    //    }
                    //}

                    p.WaitForExit();
                }

            }
            catch (Exception)
            {
                // need to do something here - this will happen async from the GUI but need way to notify
            }
        }

        /// <summary>
        /// Wrapper for the extract files method that can be used multi-threaded)
        /// </summary>
        /// <param name="keyValuePair">The checksum and the file stored in a byte array (in 7z file format)</param>
        public void extractFiles(object keyValuePair)
        {
            if (keyValuePair is KeyValuePair<string, byte[]>)
            {
                try
                {
                    extractFiles((KeyValuePair<string, byte[]>)keyValuePair);
                }
                catch (Exception exc)
                {
                    //LOG.Error(exc);
                }
            }
            else
            {
                throw new ArgumentException("The object argument must be of type KeyValuePair<string, byte[]> array!");
            }
        }

        /// <summary>
        /// Verify the zipped file's checksum and extract the files from a 7zip file stored in a byte[] in memory
        /// </summary>
        /// <param name="zippedFile">The checksum and the file stored in a byte array (in 7z file format)</param>
        /// <returns>True if the file's checksum is unchanged and the files were successfully extracted</returns>
        public bool extractFiles(KeyValuePair<string, byte[]> zippedFile)
        {
            string hashcode = Convert.ToString(Math.Abs(this.GetHashCode()));
            string fileName = EXTRACTS_DIRECTORY + hashcode + ".7z";
            IList<string> extractedFileNames = new List<string>();

            try
            {
                FileStream fs = new FileStream(fileName, FileMode.CreateNew);
                BinaryWriter bw = new BinaryWriter(fs);
                bw.Write(zippedFile.Value, 0, Convert.ToInt32(zippedFile.Value.Length));
                bw.Flush();
                bw.Close();
                fs.Close();
                // only verify MD5 if we received one
                if (!String.IsNullOrEmpty(zippedFile.Key))
                {
                    string calculatedMd5 = calculateChecksum(fileName);
                    if (calculatedMd5 != zippedFile.Key)
                    {
                        throw new ArgumentException("The zipped file checksums do not match!");
                    }
                }
                // only let one thread extract zip files
                lock (_locker)
                {
                    System.Diagnostics.Process p = new System.Diagnostics.Process();
                    p.StartInfo.FileName = "7z.exe";
                    p.StartInfo.Arguments = "e -t7z " + fileName + " -o" + EXTRACTS_DIRECTORY + " -y";
                    p.StartInfo.UseShellExecute = false;
                    p.StartInfo.RedirectStandardOutput = true;
                    p.StartInfo.CreateNoWindow = true;
                    p.StartInfo.WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden;
                    p.Start();
                    string cmdOutput = p.StandardOutput.ReadToEnd();

                    if (!String.IsNullOrEmpty(cmdOutput))
                    {
                        string[] fileNames = cmdOutput.Split(new string[] { "\r\n" }, StringSplitOptions.RemoveEmptyEntries);
                        if (fileNames != null && fileNames.Length > 0)
                        {
                            foreach (string s in fileNames)
                            {
                                if (s.Contains("Extracting "))
                                {
                                    string temp = s.Substring(s.IndexOf("  ") + 2);
                                    SavedFiles.Add(new FileInfo(temp));
                                    extractedFileNames.Add(temp);
                                    //LOG.Debug("Successfully extracted a file: " + temp);
                                }
                            }
                        }
                    }

                    p.WaitForExit();
                    File.Delete(fileName);
                    return true;
                }
            }
            catch (Exception)
            {
                try
                {
                    if (File.Exists(fileName))
                    {
                        File.Delete(fileName);
                    }
                }
                catch (Exception) { /* TBD - should we log? */ }

                if (extractedFileNames.Count > 0)
                {
                    foreach (string s in extractedFileNames)
                    {
                        try
                        {
                            FileInfo f = new FileInfo(EXTRACTS_DIRECTORY + s);
                            f.Delete();
                        }
                        catch (Exception) { /* TBD - should we log? */ }
                    }
                }
                throw;
            }
        }

        /// <summary>
        /// Compress the files from an extraction and return the zipped file's CRC and zip file as a byte[]. This code only works on
        /// files up to 2GB in size! Compresses all files matching the SITECODE_VISTAFILE_* pattern in the 
        /// downstream directory
        /// </summary>
        /// <param name="sitecode"></param>
        /// <param name="vistaFile"></param>
        /// <returns></returns>
        public KeyValuePair<string, byte[]> compressFiles(string sitecode, string vistaFile)
        {
            string fileName = EXTRACTS_DIRECTORY + Math.Abs(this.GetHashCode()).ToString() + ".7z";
            DirectoryInfo di = new DirectoryInfo(EXTRACTS_DIRECTORY);
            FileInfo[] fi = di.GetFiles(sitecode + "_" + vistaFile + "_*"); // get expected file count
            if (fi == null || fi.Length == 0)
            {
                throw new ApplicationException("You should only try compressing files if some exist in the downstream directory...");
            }

            IList<string> compressedFileNames = new List<string>();

            System.Diagnostics.Process p = new System.Diagnostics.Process();
            p.StartInfo.FileName = "7z.exe";
            p.StartInfo.Arguments = "a -t7z " + fileName + " " + EXTRACTS_DIRECTORY + sitecode + "_" + vistaFile + "_* -aoa";
            p.StartInfo.UseShellExecute = false;
            p.StartInfo.RedirectStandardOutput = true;
            p.StartInfo.CreateNoWindow = true;
            p.StartInfo.WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden;
            p.Start();
            string cmdOutput = p.StandardOutput.ReadToEnd();
            p.WaitForExit();

            if (!String.IsNullOrEmpty(cmdOutput))
            {
                string[] fileNames = cmdOutput.Split(new string[] { "\r\n" }, StringSplitOptions.RemoveEmptyEntries);
                if (fileNames != null && fileNames.Length > 0)
                {
                    foreach (string s in fileNames)
                    {
                        if (s.Contains("Compressing "))
                        {
                            string temp = s.Substring(s.IndexOf("  ") + 2);
                            SavedFiles.Add(new FileInfo(temp));
                            compressedFileNames.Add(temp);
                        }
                    }
                }
            }
            else
            {
                throw new ApplicationException("No output was generated by 7zip!");
            }

            if (compressedFileNames.Count != fi.Length)
            {
                throw new ApplicationException("7zip did not compress the same number of files as it should have"); // two different operations didn't use the same number of files!
            }

            // wait for 7zip to finish and the file system to report the file has been created
            int waitTimes = 0;
            while (!File.Exists(fileName) && waitTimes < 3)
            {
                waitTimes++;
                System.Threading.Thread.Sleep(500);
            }

            // get checksum before we delete file
            string checksum = calculateChecksum(fileName);

            FileInfo f = new FileInfo(fileName);
            byte[] buffer = new byte[f.Length];
            FileStream fs = new FileStream(fileName, FileMode.Open);
            fs.Read(buffer, 0, Convert.ToInt32(f.Length));
            fs.Close();
            File.Delete(fileName);

            return new KeyValuePair<string, byte[]>(checksum, buffer);
        }

        /// <summary>
        /// Calculate the checksum of a file
        /// </summary>
        /// <param name="filename">The full file path</param>
        /// <returns>The 32 character MD5 file checksum</returns>
        public string calculateChecksum(string filename)
        {
            if (String.IsNullOrEmpty(filename) || !File.Exists(filename))
            {
                throw new ArgumentException("Must supply a valid file name!");
            }
            System.Diagnostics.Process crcProcess = new System.Diagnostics.Process();
            crcProcess.StartInfo.FileName = "md5.exe";
            crcProcess.StartInfo.Arguments = filename;
            crcProcess.StartInfo.UseShellExecute = false;
            crcProcess.StartInfo.RedirectStandardOutput = true;
            crcProcess.StartInfo.CreateNoWindow = true;
            crcProcess.StartInfo.WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden;
            crcProcess.Start();
            string crcOutput = crcProcess.StandardOutput.ReadToEnd();
            crcProcess.WaitForExit();
            return crcOutput.Substring(0, 32);
        }

        public IList<FileInfo> getExtractedFilesForBatchId(String batchId)
        {
            IList<FileInfo> result = new List<FileInfo>();

            IList<String> siteDirs = new List<String>(Directory.GetDirectories(this.BASE_DIRECTORY + "source\\"));
            // if we got any results, we can start recursing
            if (siteDirs != null && siteDirs.Count > 0)
            {
                foreach (String siteDir in siteDirs)
                {
                    IList<DirectoryInfo> batchDirs = getSubDirs(siteDir);
                    if (batchDirs != null && batchDirs.Count > 0)
                    {
                        foreach (DirectoryInfo batchDir in batchDirs)
                        {
                            if (!String.Equals(batchDir.Name, batchId)) // <waves hand>this is not the batch you are looking for...</waves hand>
                            {
                                continue;
                            }
                            String[] allFilesInSiteAndBatch = Directory.GetFiles(batchDir.FullName);
                            if (allFilesInSiteAndBatch != null && allFilesInSiteAndBatch.Length > 0)
                            {
                                foreach (String file in allFilesInSiteAndBatch)
                                {
                                    result.Add(new FileInfo(file));
                                }
                            }
                        }
                    }
                }
            }

            return result;
        }

        public Dictionary<string, IList<FileInfo>> getExtractedFilesByBatchId()
        {
            Dictionary<String, IList<FileInfo>> result = new Dictionary<string, IList<FileInfo>>();
            // start at base dir so we can get sub dir info
            IList<String> siteDirs = new List<String>(Directory.GetDirectories(this.BASE_DIRECTORY + "source\\")); 
            // if we got any results, we can start recursing
            if (siteDirs != null && siteDirs.Count > 0)
            {
                foreach (String siteDir in siteDirs)
                {
                    IList<DirectoryInfo> batchDirs = getSubDirs(siteDir);
                    if (batchDirs != null && batchDirs.Count > 0)
                    {
                        foreach (DirectoryInfo batchDir in batchDirs)
                        {
                            if (!result.ContainsKey(batchDir.Name))
                            {
                                result.Add(batchDir.Name, new List<FileInfo>());
                            }
                            String[] allFilesInSiteAndBatch = Directory.GetFiles(batchDir.FullName);
                            if (allFilesInSiteAndBatch != null && allFilesInSiteAndBatch.Length > 0)
                            {
                                foreach (String file in allFilesInSiteAndBatch)
                                {
                                    result[batchDir.Name].Add(new FileInfo(file));
                                }
                            }
                        }
                    }
                }
            }
            return result;            
        }

        public IList<DirectoryInfo> getSubDirs(String basePath)
        {
            IList<String> dirs = new List<String>(Directory.GetDirectories(basePath));
            IList<DirectoryInfo> dirsInfo = new List<DirectoryInfo>();
            foreach (String siteDir in dirs)
            {
                DirectoryInfo di = new DirectoryInfo(siteDir);
                dirsInfo.Add(di);
            }
            return dirsInfo;
        }

        public IList<FileInfo> searchDirectoryForFile(String baseDirectory, String fileMatchString)
        {
            DirectoryInfo di = new DirectoryInfo(baseDirectory);
            return di.GetFiles(fileMatchString).ToList<FileInfo>();
        }


        public void deleteDirectoriesForBatch(String baseDirectory, String batchId)
        {
            DirectoryInfo[] matches = new DirectoryInfo(baseDirectory).GetDirectories(batchId, SearchOption.AllDirectories);
            if (matches == null || matches.Length == 0)
            {
                return;
            }
            for (int i = 0; i < matches.Length; i++)
            {
                matches[i].Delete(true);
            }
        }
    }
}
