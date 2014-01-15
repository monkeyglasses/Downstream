using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;

namespace com.bitscopic.downstream.dao.file
{
    public class FileDaoFactory
    {
        public FileDaoFactory() { }

        public IFileDao getFileDao()
        {
            String fileDaoType = ConfigurationManager.AppSettings[config.AppConfigSettingsConstants.FileDaoType];
            if (String.IsNullOrEmpty(fileDaoType))
            {
                fileDaoType = Enum.GetName(typeof(FileDaoTypes), FileDaoTypes.LOCAL_FILE); // default to local file DAO type
            }

            FileDaoTypes daoType = (FileDaoTypes)Enum.Parse(typeof(FileDaoTypes), fileDaoType, true);

            if (daoType == FileDaoTypes.LOCAL_FILE)
            {
                return new FileDao();
            }
            else if (daoType == FileDaoTypes.NETWORK)
            {
                return new NetworkFileDao();
            }
            else
            {
                throw new NotImplementedException("That file DAO type has not been implemented");
            }
        }

        public IFileDao getFileDao(FileDaoTypes daoType)
        {
            if (daoType == FileDaoTypes.LOCAL_FILE)
            {
                return new FileDao();
            }
            else if (daoType == FileDaoTypes.NETWORK)
            {
                return new NetworkFileDao();
            }
            else
            {
                throw new NotImplementedException("That file DAO type has not been implemented");
            }
        }
    }

    public enum FileDaoTypes
    {
        LOCAL_FILE,
        NETWORK
    }
}
