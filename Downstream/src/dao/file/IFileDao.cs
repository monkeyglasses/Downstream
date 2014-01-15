using System;
using System.Collections.Generic;
using System.IO;
using com.bitscopic.downstream.service;
using com.bitscopic.downstream.dao.file;
using System.Data;

namespace com.bitscopic.downstream.dao.file
{
    public interface IFileDao
    {
        void deleteDirectoriesForBatch(String baseDirectory, String batchId);
        void createTriggerForSiteAndBatch(String triggerDirectory, String tempArchivePathUnixFormat, String siteId, String downstreamBatchId, String[] flags);
        IList<FileInfo> searchDirectoryForFile(String baseDirectory, String fileMatchString);
        void decompressZipFile(String filePath, String destinationDirectory);
        IList<FileInfo> getExtractedFilesForBatchId(String batchId);
        Dictionary<string, IList<FileInfo>> getExtractedFilesByBatchId();
        void createMarkerFile();
        void setExtractsDirectory(String sitecode, String subDirectory);
        string calculateChecksum(string filename);
        KeyValuePair<string, byte[]> compressFiles(string sitecode, string vistaFile);
        void deleteAllDownstreamFiles();
        IList<FileInfo> DeletedFiles { get; set; }
        void deleteFiles(IList<FileInfo> files);
        void deleteFilesFromSite(string sitecode);
        void deleteFilesFromSite(string sitecode, string vistaFile);
        void deleteFilesFromSiteInFolder(string fileNameBeginning, DirectoryInfo folderPath);
        string DownstreamBaseDirectory { get; }
        string DownstreamExtractsDirectory { get; }
        bool extractFiles(KeyValuePair<string, byte[]> zippedFile);
        void extractFiles(object keyValuePair);
        IList<FileInfo> getFiles();
        string getLastIen(string siteCode, string vistaFile);
        string getRandomFile();
        KeyValuePair<string, string> getSiteAndFileFromFileName(string filePath);
        ISet<string> getWorkingSites();
        IList<DataTable> loadFromAllFiles(string sitecode);
        IList<DataTable> loadFromAllFiles(string sitecode, ExtractorMode type);
        IList<DataTable> loadFromAllFiles(string sitecode, string vistaFile, ExtractorMode type);
        DataTable loadFromFile(string fileName);
        DataTable loadFromFile(string fileName, bool usingEncryption);
        IList<FileInfo> OpenedFiles { get; set; }
        string readFile(string filePath);
        IList<FileInfo> SavedFiles { get; set; }
        int SavedFilesTotalBytes { get; set; }
        string saveToFile(DataTable table, ExtractorMode type);
        string saveToFile(DataTable table, ExtractorMode type, bool usingEncryption);
        string saveToFile(object graph, string fileName);
        void saveFilesWithRollback(IEnumerable<DataTable> tables, ExtractorMode extractMode, Int32 maxTries);
    }
}
