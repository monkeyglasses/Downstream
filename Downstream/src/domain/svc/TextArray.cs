using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace com.bitscopic.downstream.domain.svc
{
    [Serializable]
    public class TextArray : AbstractArrayTO
    {
        public String[] text;

        public TextArray() { }
    }
}
