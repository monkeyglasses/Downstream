using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using com.bitscopic.downstream.service;
using com.bitscopic.downstream.src.utils;
using System.Configuration;
using System.IO;
using System.Data;

namespace com.bitscopic.downstream.dao.file
{
    public class NetworkFileDao : FileDao, IFileDao
    {
        String _networkUserName;
        String _networkUserPwd;
        String _networkUserDomain;

        public NetworkFileDao() : base(false)
        {
            _networkUserDomain = ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.NetworkUserDomain];
            _networkUserName = ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.NetworkUserName];
            _networkUserPwd = ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.NetworkUserPassword];
        }

        public new void createMarkerFile()
        {
            using (ImpersonatorUtils impersonator = new ImpersonatorUtils())
            {
                impersonator.startImpersonation(_networkUserDomain, _networkUserName, _networkUserPwd);
                base.createMarkerFile();
            }
        }

        public new void setExtractsDirectory(String sitecode, String subDirectory)
        {
            using (ImpersonatorUtils impersonator = new ImpersonatorUtils())
            {
                impersonator.startImpersonation(_networkUserDomain, _networkUserName, _networkUserPwd);
                base.setExtractsDirectory(sitecode, subDirectory);
            }
        }

        public new string calculateChecksum(string filename)
        {
            using (ImpersonatorUtils impersonator = new ImpersonatorUtils())
            {
                impersonator.startImpersonation(_networkUserDomain, _networkUserName, _networkUserPwd);
                return base.calculateChecksum(filename);
            }
        }

        public new KeyValuePair<string, byte[]> compressFiles(string sitecode, string vistaFile)
        {
            using (ImpersonatorUtils impersonator = new ImpersonatorUtils())
            {
                impersonator.startImpersonation(_networkUserDomain, _networkUserName, _networkUserPwd);
                return base.compressFiles(sitecode, vistaFile);
            }
        }

        public new void deleteAllDownstreamFiles()
        {
            using (ImpersonatorUtils impersonator = new ImpersonatorUtils())
            {
                impersonator.startImpersonation(_networkUserDomain, _networkUserName, _networkUserPwd);
                base.deleteAllDownstreamFiles();
            }
        }

        public new void deleteFiles(IList<System.IO.FileInfo> files)
        {
            using (ImpersonatorUtils impersonator = new ImpersonatorUtils())
            {
                impersonator.startImpersonation(_networkUserDomain, _networkUserName, _networkUserPwd);
                base.deleteFiles(files);
            }
        }

        public new void deleteFilesFromSite(string sitecode)
        {
            using (ImpersonatorUtils impersonator = new ImpersonatorUtils())
            {
                impersonator.startImpersonation(_networkUserDomain, _networkUserName, _networkUserPwd);
                base.deleteFilesFromSite(sitecode);
            }
        }

        public new void deleteFilesFromSite(string sitecode, string vistaFile)
        {
            using (ImpersonatorUtils impersonator = new ImpersonatorUtils())
            {
                impersonator.startImpersonation(_networkUserDomain, _networkUserName, _networkUserPwd);
                base.deleteFilesFromSite(sitecode, vistaFile);
            }
        }

        public new void deleteFilesFromSiteInFolder(string fileNameBeginning, System.IO.DirectoryInfo folderPath)
        {
            using (ImpersonatorUtils impersonator = new ImpersonatorUtils())
            {
                impersonator.startImpersonation(_networkUserDomain, _networkUserName, _networkUserPwd);
                base.deleteFilesFromSiteInFolder(fileNameBeginning, folderPath);
            }
        }

        public new bool extractFiles(KeyValuePair<string, byte[]> zippedFile)
        {
            using (ImpersonatorUtils impersonator = new ImpersonatorUtils())
            {
                impersonator.startImpersonation(_networkUserDomain, _networkUserName, _networkUserPwd);
                return base.extractFiles(zippedFile);
            }
        }

        public new void extractFiles(object keyValuePair)
        {
            using (ImpersonatorUtils impersonator = new ImpersonatorUtils())
            {
                impersonator.startImpersonation(_networkUserDomain, _networkUserName, _networkUserPwd);
                base.extractFiles(keyValuePair);
            }
        }

        public new IList<System.IO.FileInfo> getFiles()
        {
            using (ImpersonatorUtils impersonator = new ImpersonatorUtils())
            {
                impersonator.startImpersonation(_networkUserDomain, _networkUserName, _networkUserPwd);
                return base.getFiles();
            }
        }

        public new string getLastIen(string siteCode, string vistaFile)
        {
            using (ImpersonatorUtils impersonator = new ImpersonatorUtils())
            {
                impersonator.startImpersonation(_networkUserDomain, _networkUserName, _networkUserPwd);
                return base.getLastIen(siteCode, vistaFile);
            }
        }

        public new string getRandomFile()
        {
            using (ImpersonatorUtils impersonator = new ImpersonatorUtils())
            {
                impersonator.startImpersonation(_networkUserDomain, _networkUserName, _networkUserPwd);
                return base.getRandomFile();
            }
        }

        public new KeyValuePair<string, string> getSiteAndFileFromFileName(string filePath)
        {
            using (ImpersonatorUtils impersonator = new ImpersonatorUtils())
            {
                impersonator.startImpersonation(_networkUserDomain, _networkUserName, _networkUserPwd);
                return base.getSiteAndFileFromFileName(filePath);
            }
        }

        public new ISet<string> getWorkingSites()
        {
            using (ImpersonatorUtils impersonator = new ImpersonatorUtils())
            {
                impersonator.startImpersonation(_networkUserDomain, _networkUserName, _networkUserPwd);
                return base.getWorkingSites();
            }
        }

        public new IList<System.Data.DataTable> loadFromAllFiles(string sitecode)
        {
            using (ImpersonatorUtils impersonator = new ImpersonatorUtils())
            {
                impersonator.startImpersonation(_networkUserDomain, _networkUserName, _networkUserPwd);
                return base.loadFromAllFiles(sitecode);
            }
        }

        public new IList<System.Data.DataTable> loadFromAllFiles(string sitecode, ExtractorMode type)
        {
            using (ImpersonatorUtils impersonator = new ImpersonatorUtils())
            {
                impersonator.startImpersonation(_networkUserDomain, _networkUserName, _networkUserPwd);
                return base.loadFromAllFiles(sitecode, type);
            }
        }

        public new IList<System.Data.DataTable> loadFromAllFiles(string sitecode, string vistaFile, ExtractorMode type)
        {
            using (ImpersonatorUtils impersonator = new ImpersonatorUtils())
            {
                impersonator.startImpersonation(_networkUserDomain, _networkUserName, _networkUserPwd);
                return base.loadFromAllFiles(sitecode, vistaFile, type);
            }
        }

        public new System.Data.DataTable loadFromFile(string fileName)
        {
            using (ImpersonatorUtils impersonator = new ImpersonatorUtils())
            {
                impersonator.startImpersonation(_networkUserDomain, _networkUserName, _networkUserPwd);
                return base.loadFromFile(fileName);
            }
        }

        public new System.Data.DataTable loadFromFile(string fileName, bool usingEncryption)
        {
            using (ImpersonatorUtils impersonator = new ImpersonatorUtils())
            {
                impersonator.startImpersonation(_networkUserDomain, _networkUserName, _networkUserPwd);
                return base.loadFromFile(fileName, usingEncryption);
            }
        }

        public new string readFile(string filePath)
        {
            using (ImpersonatorUtils impersonator = new ImpersonatorUtils())
            {
                impersonator.startImpersonation(_networkUserDomain, _networkUserName, _networkUserPwd);
                return base.readFile(filePath);
            }
        }

        public new string saveToFile(System.Data.DataTable table, ExtractorMode type)
        {
            using (ImpersonatorUtils impersonator = new ImpersonatorUtils())
            {
                impersonator.startImpersonation(_networkUserDomain, _networkUserName, _networkUserPwd);
                return base.saveToFile(table, type);
            }
        }

        public new string saveToFile(System.Data.DataTable table, ExtractorMode type, bool usingEncryption)
        {
            using (ImpersonatorUtils impersonator = new ImpersonatorUtils())
            {
                impersonator.startImpersonation(_networkUserDomain, _networkUserName, _networkUserPwd);
                return base.saveToFile(table, type, usingEncryption);
            }
        }

        public new String saveToFile(object graph, String fileName)
        {
            using (ImpersonatorUtils impersonator = new ImpersonatorUtils())
            {
                impersonator.startImpersonation(_networkUserDomain, _networkUserName, _networkUserPwd);
                return base.saveToFile(graph, fileName);
            }
        }

        public new String saveToFile(object graph, String fileName, bool usingEncryption)
        {
            using (ImpersonatorUtils impersonator = new ImpersonatorUtils())
            {
                impersonator.startImpersonation(_networkUserDomain, _networkUserName, _networkUserPwd);
                return base.saveToFile(graph, fileName, usingEncryption);
            }
        }

        public new Dictionary<string, IList<FileInfo>> getExtractedFilesByBatchId()
        {
            using (ImpersonatorUtils impersonator = new ImpersonatorUtils())
            {
                impersonator.startImpersonation(_networkUserDomain, _networkUserName, _networkUserPwd);
                return base.getExtractedFilesByBatchId();
            }
        }

        public new IList<FileInfo> getExtractedFilesForBatchId(String batchId)
        {
            using (ImpersonatorUtils impersonator = new ImpersonatorUtils())
            {
                impersonator.startImpersonation(_networkUserDomain, _networkUserName, _networkUserPwd);
                return base.getExtractedFilesForBatchId(batchId);
            }
        }

        public new void decompressZipFile(String filePath, String destinationDirectory)
        {
            using (ImpersonatorUtils impersonator = new ImpersonatorUtils())
            {
                impersonator.startImpersonation(_networkUserDomain, _networkUserName, _networkUserPwd);
                base.decompressZipFile(filePath, destinationDirectory);
            }
        }

        public IList<FileInfo> searchDirectoryForFile(String baseDirectory, String fileMatchString)
        {
            using (ImpersonatorUtils impersonator = new ImpersonatorUtils())
            {
                impersonator.startImpersonation(_networkUserDomain, _networkUserName, _networkUserPwd);
                return base.searchDirectoryForFile(baseDirectory, fileMatchString);
            }
        }

        public void createTriggerForSiteAndBatch(String triggerDirectory, String tempArchivePathUnixFormat, String siteId, String downstreamBatchId, String[] flags)
        {
            using (ImpersonatorUtils impersonator = new ImpersonatorUtils())
            {
                impersonator.startImpersonation(_networkUserDomain, _networkUserName, _networkUserPwd);
                base.createTriggerForSiteAndBatch(triggerDirectory, tempArchivePathUnixFormat, siteId, downstreamBatchId, flags);
            }
        }

        public new void deleteDirectoriesForBatch(String baseDirectory, String batchId)
        {
            using (ImpersonatorUtils impersonator = new ImpersonatorUtils())
            {
                impersonator.startImpersonation(_networkUserDomain, _networkUserName, _networkUserPwd);
                base.deleteDirectoriesForBatch(baseDirectory, batchId);
            }
        }

        public new void saveFilesWithRollback(IEnumerable<DataTable> tables, ExtractorMode extractMode, Int32 maxTries)
        {
            using (ImpersonatorUtils impersonator = new ImpersonatorUtils())
            {
                impersonator.startImpersonation(_networkUserDomain, _networkUserName, _networkUserPwd);
                base.saveFilesWithRollback(tables, extractMode, maxTries);
            }
        }
    }
}
