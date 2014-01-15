using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using com.bitscopic.downstream.dao.file;
using System.Configuration;

namespace com.bitscopic.downstream.config
{
    public class PackageTranslator
    {
        internal static Dictionary<String, IList<String>> _packageDictionary;

        public PackageTranslator() 
        {
            //String fileContents = new FileDao().readFile(ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.PackageMappingConfig]);
            //_packageDictionary = parseMappingContents(fileContents);
        }

        public PackageTranslator(String mappingFilePath)
        {
            String fileContents = new FileDao().readFile(mappingFilePath);
            _packageDictionary = parseMappingContents(fileContents);
        }

        internal Dictionary<String, IList<String>> parseMappingContents(String fileContents)
        {
            if (String.IsNullOrEmpty(fileContents))
            {
                throw new ArgumentNullException("Package mapping file contents appear to be empty");
            }

            String[] lines = fileContents.Split(new String[] { Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries);
            String currentPackageName = "";
            Dictionary<String, IList<String>> packages = new Dictionary<string, IList<string>>();

            for (int i = 0; i < lines.Length; i++)
            {
                if (lines[i].StartsWith("//"))
                {
                    continue; // comments - ignore them
                }
                if (lines[i].Contains('[') && lines[i].Contains(']'))
                {
                    currentPackageName = lines[i].Trim().Replace("[", "").Replace("]", "");
                    packages.Add(currentPackageName, new List<String>());
                    continue;
                }
                if (String.IsNullOrEmpty(currentPackageName))
                {
                    continue;
                }

                string fileNumber = lines[i].Split(new char[] { ' ' })[0].Trim();
                packages[currentPackageName].Add(fileNumber);
            }
            return packages;
        }

        public IList<String> getFilesInPackage(String packageName)
        {
            return _packageDictionary[packageName];
        }

        public String getPackageForFile(String fileNumber)
        {
            foreach (String key in _packageDictionary.Keys)
            {
                foreach (String dictFileNum in _packageDictionary[key])
                {
                    if (String.Equals(fileNumber, dictFileNum))
                    {
                        return key;
                    }
                }
            }
            return String.Format("File number not found in any package - {0}", fileNumber);
        }

        public Dictionary<String, IList<String>> getPackages()
        {
            return _packageDictionary;
        }
    }
}
