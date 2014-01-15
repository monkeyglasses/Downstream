using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using com.bitscopic.downstream.domain;
using com.bitscopic.downstream.domain.reporting;

namespace com.bitscopic.downstream.dao.vista
{
    public interface IVistaDao
    {
        String[] ddrLister(String siteId, String vistaFile, String iens, String fields, String flags, String maxRex,
                                        String from, String part, String xRef, String screen, String identifier);

        String[] ddrGetsEntry(String siteId, String vistaFile, String iens, String flds, String flags);

        String getVariableValueQuery(String siteId, string arg);
    }
}
