using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace com.bitscopic.downstream.domain.svc
{
    [Serializable]
    public class TextTO : AbstractTO
    {
        public String text;

        public TextTO() { }
    }
}
