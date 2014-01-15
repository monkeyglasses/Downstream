using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using com.bitscopic.downstream.domain;

namespace com.bitscopic.downstream.service
{
    interface IService
    {
        ServiceState getServiceState();
        ServiceState setServiceState(ServiceStatus setTo);
        void execute();
    }
}
