using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace com.bitscopic.downstream.domain.reporting
{
    public class ThreadSafeReport : Report
    {
        private static readonly object _locker = new object();

        public ThreadSafeReport() : base("") { }

        public new void addException(Exception exc)
        {
            lock (_locker)
            {
                base.addException(exc);
            }
        }

        public new void addError(String message)
        {
            lock (_locker)
            {
                base.addError(message);
            }
        }

        public new void addError(String message, Exception exc)
        {
            lock (_locker)
            {
                base.addError(message, exc);
            }
        }

        public new void addInfo(String message)
        {
            lock (_locker)
            {
                base.addInfo(message);
            }
        }

        public new void addDebug(String message)
        {
            lock (_locker)
            {
                base.addDebug(message);
            }
        }
    }
}
