using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace com.bitscopic.downstream.dao.vista
{
    public class VistaDaoFactory
    {
        public IVistaDao getVistaDao(String daoType)
        {
            if (String.Equals("MdoVistaDao", daoType, StringComparison.CurrentCultureIgnoreCase))
            {
                return MdoVistaDao.getInstance();
            }
            else if (String.Equals("WcfVistaDao", daoType, StringComparison.CurrentCultureIgnoreCase))
            {
                return new WcfVistaDao();
            }
            else if (String.Equals("MockVistaDao", daoType, StringComparison.CurrentCultureIgnoreCase))
            {
                return new MockVistaDao();
            }
            else
            {
                throw new ArgumentException("Invalid Vista DAO type!");
            }
        }
    }
}
