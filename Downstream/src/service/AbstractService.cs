using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace com.bitscopic.downstream.service
{
    [Serializable]
    public enum ExtractorMode
    {
        INCREMENTAL,
        REBUILD,
        DIFF
    }

    [Serializable]
    public enum ServiceType
    {
        VistaService,
        SqlService,
        OrchestratorService
    }

    public abstract class AbstractService : IService
    {
        public virtual void execute()
        {
            start();
            run();
            stop();
        }

        internal abstract void shutdown();

        protected abstract void start();
        protected abstract void run();
        protected abstract void stop();

        public abstract domain.ServiceState getServiceState();
        public abstract domain.ServiceState setServiceState(domain.ServiceStatus setTo);

        public String Name { get; set; }
    }

    public class ServiceFactory
    {
        public AbstractService getService(string serviceType)
        {
            if (String.IsNullOrEmpty(serviceType))
            {
                throw new ArgumentNullException("No service type specified!");
            }

            ServiceType myType = ServiceType.OrchestratorService;
            if (!Enum.TryParse<ServiceType>(serviceType, true, out myType))
            {
                throw new ArgumentException("Invalid service type specified: " + serviceType);
            }
            return getService(myType);
        }

        public AbstractService getService(ServiceType serviceType)
        {
            throw new NotImplementedException();
            if (serviceType == ServiceType.VistaService)
            {
                //return new VistaService();
            }
            else if (serviceType == ServiceType.SqlService)
            {
                //return new SqlService();
            }
            else if (serviceType == ServiceType.OrchestratorService)
            {
                //return new OrchestratorService();
            }
            else
            {
                throw new NotImplementedException("That service type has not been implemented!");
            }
        }
    }
}
