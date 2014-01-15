using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using com.bitscopic.downstream.net;

namespace com.bitscopic.downstream.domain.exception
{
    public class DownstreamException : ApplicationException
    {
        public Extractor Extractor { get; set; }
        public ExtractorConfiguration ExtractorConfiguration { get; set; }
        public VistaQuery VistaQuery { get; set; }
        public MessageTO MessageTO { get; set; }
        public Exception InnerException { get; set; }

        public DownstreamException()
            : base()
        {

        }

        public DownstreamException(string message)
            : base(message)
        {

        }

        public DownstreamException(string message, Exception inner)
            : base(message, inner)
        {

        }

        /// <summary>
        /// Downstream extractor constructor. Any of the domain specific arguments can be null
        /// </summary>
        /// <param name="extractor"></param>
        /// <param name="extractorConfig"></param>
        /// <param name="vistaQuery"></param>
        /// <param name="messageTO"></param>
        /// <param name="message"></param>
        /// <param name="inner"></param>
        public DownstreamException(Extractor extractor, ExtractorConfiguration extractorConfig, VistaQuery vistaQuery,
            MessageTO messageTO, string message, Exception inner)
            : base(message, inner)
        {
            Extractor = extractor;
            ExtractorConfiguration = extractorConfig;
            VistaQuery = vistaQuery;
            MessageTO = messageTO;
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("***** Start DownstreamException String *****");
            if (!String.IsNullOrEmpty(Message))
            {
                sb.AppendLine("Message: " + Message);
            }
            if (InnerException != null)
            {
                sb.AppendLine("Inner Exception: " + InnerException.ToString());
            }
            if (Extractor != null)
            {
                sb.AppendLine("Extractor: " + Extractor.ToString());
            }
            if (VistaQuery != null)
            {
                sb.AppendLine("Vista Query: " + VistaQuery.ToString());
            }
            if (ExtractorConfiguration != null)
            {
                sb.AppendLine("Extractor Config: " + ExtractorConfiguration.ToString());
            }
            if (MessageTO != null)
            {
                sb.AppendLine("MessageTO Object: " + MessageTO.ToString());
            }
            sb.AppendLine("***** End DownstreamException String *****");
            return sb.ToString();
        }
    }
}
