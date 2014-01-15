using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Serialization;

namespace com.bitscopic.downstream.domain.svc
{
    [Serializable]
    public class BoolTO : AbstractTO
    {
        public bool tf;
    }
}
