using System;

namespace com.bitscopic.downstream.domain.exception
{
    public class DownstreamFileTransactionException : DownstreamException 
    {
        public DownstreamFileTransactionException(String message) : base(message) { } 
    }
}
