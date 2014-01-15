using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using com.bitscopic.downstream.net;

namespace com.bitscopic.downstream.net
{
    public class ExtractorServer : Server
    {
        public static Int32 ExtractorPercentageComplete { get; set; }

        public ExtractorServer() : base() { }

        public ExtractorServer(bool randomPort) : base(randomPort) { }
    }
}
