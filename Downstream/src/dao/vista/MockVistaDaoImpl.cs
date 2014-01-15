using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using com.bitscopic.downstream.domain;

namespace com.bitscopic.downstream.dao.vista
{
    public class MockVistaDaoImpl : VistaDaoImpl
    {
        public QueryResults queryWithDepth(ExtractorConfiguration config, VistaQuery topLevelQuery)
        {
            throw new NotImplementedException(); 
        }
    }
}
