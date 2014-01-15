using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using com.bitscopic.downstream.dao.vista;
using com.bitscopic.downstream.domain.exception;

namespace com.bitscopic.downstream.utils
{
    public class BinarySearchUtil
    {
        IVistaDao _dao;
        const int PRECISION = 8;
        int _iterations = 0;

        public BinarySearchUtil(IVistaDao dao)
        {
            _dao = dao;
        }

        public String getMaxRexForLastQuery(String startPoint, String maxRex)
        {
            String endPoint = getEndPointFromBinarySearch(startPoint, maxRex);
            return (Convert.ToDecimal(endPoint) - Convert.ToDecimal(startPoint)).ToString();
        }

        /// <summary>
        /// Ticket #120 - this algorithm is used to help determine how many records
        /// to pull from the NEW PERSON file in site 583 (Indy)
        /// </summary>
        /// <returns></returns>
        public String getEndPointFromBinarySearch(String startPoint, String maxRex)
        {
            _iterations++;
            
            Int32 maxRexAsInt = Convert.ToInt32(maxRex);
            if ((maxRexAsInt / 2) < 3 || _iterations > PRECISION) // if we're within a few IENs, go ahead and return start point
            {
                return startPoint;
            }
            try
            {
                String[] testQuery = _dao.ddrLister("583", "200", "", ".01", "IP", maxRex, startPoint, "", "#", "", "");
                String lastSuccessfulIen = testQuery[testQuery.Length - 1].Split(new char[] { '^' })[0];
                return getEndPointFromBinarySearch(lastSuccessfulIen, (maxRexAsInt / 2).ToString());
            }
            catch (gov.va.medora.mdo.exceptions.MdoException mdoExc)
            {
                if (mdoExc.Message.Contains("M  ERROR"))
                {
                    return getEndPointFromBinarySearch(startPoint, (maxRexAsInt/2).ToString());
                }
                else
                {
                    throw mdoExc;
                }
            }
            catch (Exception)
            {
                throw;
            }
            throw new BinarySearchException();
        }
    }
}
